from pyicloud import PyiCloudService
import pandas as pd
# import zipfile, io
import os
from xml.etree.ElementTree import iterparse

from gpxtocsv import gpxtolist

from prefect import task, Flow, Parameter, tasks
from prefect.tasks.gcp.storage import GCSUpload
from prefect.tasks.gcp.bigquery import BigQueryLoadGoogleCloudStorage
from prefect.tasks.secrets import PrefectSecret

from datetime import timedelta, datetime
from prefect.schedules import IntervalSchedule

schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=1),
)

# try to pull in the export file and persist it, return it
# then use it to feed transform data functions which will return pd dataframes

@task
def extract_health_data(u,p):
    # fetch the export.zip file from iCloud
    # authenticate into iCloud
    api = PyiCloudService(u,p)
    # grab the health data zip file and extract xml and gpx data
    export = api.drive['Documents']['health_data']['export.zip'].open(stream=True) # get http response object
    # save zip file locally
    #z = zipfile.ZipFile(io.BytesIO(export.content))
    # looks like we'll have to write the zip file and unzip with prefect functions
    file = open("export.zip","wb")
    file.write(export.content)
    file.close()
    unzip = tasks.files.compression.Unzip(zip_path="export.zip")
    unzip.run()
    return

@task
def transform_workout_data(nothing):
    # load .zip contents from local path with zipfile package
    li = []
    context = iterparse(open('apple_health_export/export.xml'), events=("start","end"))
    for index, (event, elem) in enumerate(context):
        if index == 0:
            root = elem
        if event == "end" and elem.tag == "Workout":
            li.append(elem.attrib)
            root.clear()
    df = pd.DataFrame(li)
    # li = []
    # for _, elem in iterparse(open('apple_health_export/export.xml')):
    #     if elem.tag == "Workout":
    #         li.append(elem.attrib)
    #         elem.clear()
    # df = pd.DataFrame(li)
    return df

@task
def transform_gpx_data(nothing):
    # parse .gpx data
    # change gpxtocsv back to original format which intakes a xml file instead of zip
    #df = pd.concat([pd.DataFrame(gpxtolist(z,x)) for x in z.namelist() if x.endswith(".gpx")])
    # for x in os.listdir("apple_health_export/workout-routes"):
    #     if x.endswith(".gpx"):
    #         df = pd.concat([pd.DataFrame(gpxtolist(open("apple_health_export/workout-routes/"+x)))])
    
    # df = pd.concat([pd.DataFrame(gpxtolist(open("apple_health_export/workout-routes/"+x).read())) for x in os.listdir("apple_health_export/workout-routes") if x.endswith(".gpx")])
    df = pd.concat([pd.DataFrame(gpxtolist("apple_health_export/workout-routes/"+x)) for x in os.listdir("apple_health_export/workout-routes")])

    return df

@task
def load_health_data(df,project_id, bucket_id):
    blob_name = GCSUpload(bucket=bucket_id, project=project_id, blob="workout")
    d = df.to_csv()
    d = d[1:]
    blob_name = blob_name.run(data=d)
    return blob_name

@task
def load_gpx_data(df,project_id, bucket_id):
    blob_name = GCSUpload(bucket=bucket_id, project=project_id, blob="gpx")
    d = df.to_csv()
    d = d[1:]
    blob_name = blob_name.run(data=d)
    return blob_name

# add two tasks that will send a request to GCP to run a job that connects GCS to BQ tables
# to find the blob name, return the value from the load tasks
@task
def push_health_data(blob_name, bucket_id, project_id, table_id, dataset_id):
    uri_cat = "gs://" + bucket_id + "/" + blob_name
    job = BigQueryLoadGoogleCloudStorage(uri=uri_cat, project=project_id, dataset_id=dataset_id, table=table_id)
    job.run(write_disposition="WRITE_TRUNCATE", source_format="CSV")
    return

@task
def push_gpx_data(blob_name, bucket_id, project_id, table_id, dataset_id):
    uri_cat = "gs://" + bucket_id + "/" + blob_name
    job = BigQueryLoadGoogleCloudStorage(uri=uri_cat, project=project_id,dataset_id=dataset_id, table=table_id)
    job.run(write_disposition="WRITE_TRUNCATE", source_format="CSV")
    return

# @task
# def remove_objs():
#     return

with Flow("AppleHealth-ETL") as flow:
    # sets dependencies, it DOES NOT run the functions below
    # set secrets via export PREFECT__CONTEXT__SECRETS__MY_KEY="MY_VALUE"
    project_id = PrefectSecret('PROJECTID')
    bucket_id = PrefectSecret('BUCKETID')
    u = PrefectSecret('ICLOUDU')
    p = PrefectSecret('ICLOUDP')
    dataset_id = PrefectSecret('DATASETID')
    health_tableid = PrefectSecret('HTABLEID')
    gpx_tableid = PrefectSecret('GPXTABLEID')
    apple_data = extract_health_data(u,p)

    transformed_workout_data = transform_workout_data(apple_data)
    transformed_gpx_data = transform_gpx_data(apple_data)

    # fix these functions, I think you have to use the prefect functions to work
    workout_blob = load_health_data(transformed_workout_data, project_id, bucket_id)
    gpx_blob = load_gpx_data(transformed_gpx_data, project_id, bucket_id)

    # tell BQ data transfer to pick up new data
    push_health_data(workout_blob,bucket_id,project_id,health_tableid,dataset_id)
    push_gpx_data(gpx_blob, bucket_id, project_id, gpx_tableid, dataset_id)
    #flow.run()
flow.register(project_name="health-data")