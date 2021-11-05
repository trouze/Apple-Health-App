from pyicloud import PyiCloudService
import pandas as pd
import os
from xml.etree.ElementTree import iterparse

from gpxtocsv import gpxtolist

from prefect import task, Flow, Parameter, tasks
from prefect.tasks.gcp.storage import GCSUpload
from prefect.tasks.gcp.bigquery import BigQueryLoadGoogleCloudStorage
from prefect.tasks.secrets import PrefectSecret

@task
def extract_health_data(u,p):
    """
    u : iCloud username
    p : iCloud password
    ---
    Authenticates into iCloud and fetch the export.zip file
    Saves zip file to local directory
    """
    api = PyiCloudService(u,p)
    export = api.drive['Documents']['health_data']['export.zip'].open(stream=True) # get http response object
    file = open("export.zip","wb")
    file.write(export.content)
    file.close()
    unzip = tasks.files.compression.Unzip(zip_path="export.zip")
    unzip.run()
    return

@task
def transform_workout_data(nothing):
    """
    load .zip contents from local path with zipfile package and parse workout level data
    ---
    Returns tabular pandas dataframe at the date of workout level of data
    """
    li = []
    context = iterparse(open('apple_health_export/export.xml'), events=("start","end"))
    for index, (event, elem) in enumerate(context):
        if index == 0:
            root = elem
        if event == "end" and elem.tag == "Workout":
            li.append(elem.attrib)
            root.clear()
    df = pd.DataFrame(li)
    return df

@task
def transform_gpx_data(nothing):
    """
    Parses .gpx files to extract <Record> from data
    ---
    Returns tabular dataframe at the Timestamp, Lat, Long level of data
    """
    df = pd.concat([pd.DataFrame(gpxtolist("apple_health_export/workout-routes/"+x)) for x in os.listdir("apple_health_export/workout-routes")])

    return df

@task
def load_health_data(df,project_id, bucket_id):
    """
    Pushes workout data to GCS from local
    ---
    df : pandas dataframe returned from transform_workout_data()
    project_id : GCP project id
    bucket_id : GCS bucket name
    ---
    Returns : GCSUpload.run() returns the name of the blob created during the push of the file
    """
    blob_name = GCSUpload(bucket=bucket_id, project=project_id, blob="workout")
    d = df.to_csv()
    d = d[1:]
    blob_name = blob_name.run(data=d)
    return blob_name

@task
def load_gpx_data(df,project_id, bucket_id):
    """
    Pushes geo data to GCS from local
    ---
    df : pandas dataframe returned from transform_workout_data()
    project_id : GCP project id
    bucket_id : GCS bucket name
    ---
    Returns : GCSUpload.run() returns the name of the blob created during the push of the file
    """
    blob_name = GCSUpload(bucket=bucket_id, project=project_id, blob="gpx")
    d = df.to_csv()
    d = d[1:]
    blob_name = blob_name.run(data=d)
    return blob_name

@task
def push_health_data(blob_name, bucket_id, project_id, dataset_id, table_id):
    """
    Submits an API request to run a BQ data transfer service job, taking workout data on GCS and populating a BQ table
    ---
    blob_name : name of blob created during GCSUpload.run() call
    bucket_id : name of GCS bucket created in console
    project_id : ID of Google Cloud project
    dataset_id : Name of Dataset created in BigQuery
    table_id : BigQuery Table name
    """
    uri_cat = "gs://" + bucket_id + "/" + blob_name
    job = BigQueryLoadGoogleCloudStorage(uri=uri_cat, project=project_id, dataset_id=dataset_id, table=table_id)
    job.run(write_disposition="WRITE_TRUNCATE", source_format="CSV")
    return

@task
def push_gpx_data(blob_name, bucket_id, project_id, dataset_id, table_id):
    """
    Submits an API request to run a BQ data transfer service job, taking geo data on GCS and populating a BQ table
    ---
    blob_name : name of blob created during GCSUpload.run() call
    bucket_id : name of GCS bucket created in console
    project_id : ID of Google Cloud project
    dataset_id : Name of Dataset created in BigQuery
    table_id : BigQuery Table name
    """
    uri_cat = "gs://" + bucket_id + "/" + blob_name
    job = BigQueryLoadGoogleCloudStorage(uri=uri_cat, project=project_id,dataset_id=dataset_id, table=table_id)
    job.run(write_disposition="WRITE_TRUNCATE", source_format="CSV")
    return

with Flow("AppleHealth-ETL") as flow:
    # set secrets via export PREFECT__CONTEXT__SECRETS__MY_KEY="MY_VALUE"
    # get secrets
    project_id = PrefectSecret('PROJECTID')
    bucket_id = PrefectSecret('BUCKETID')
    u = PrefectSecret('ICLOUDU')
    p = PrefectSecret('ICLOUDP')
    dataset_id = PrefectSecret('DATASETID')
    health_tableid = PrefectSecret('HTABLEID')
    gpx_tableid = PrefectSecret('GPXTABLEID')
    
    # fetch export data zipfile
    apple_data = extract_health_data(u,p)
    
    # parse XML documents
    transformed_workout_data = transform_workout_data(apple_data)
    transformed_gpx_data = transform_gpx_data(apple_data)

    # push to GCS
    workout_blob = load_health_data(transformed_workout_data, project_id, bucket_id)
    gpx_blob = load_gpx_data(transformed_gpx_data, project_id, bucket_id)

    # Submit Job request to Load GCS data into BQ
    push_health_data(workout_blob, bucket_id, project_id, dataset_id, health_tableid)
    push_gpx_data(gpx_blob, bucket_id, project_id, dataset_id, gpx_tableid)
    
flow.register(project_name="health-data")
