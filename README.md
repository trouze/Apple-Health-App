# Apple-Health-App
End-to-end platform that picks up Apple Health export data from iCloud and runs a data pipeline via [Prefect](https://www.prefect.io/) to monitor data engineering tasks as they run onboard a machine running a Prefect Agent. Posts data to a BigQuery table for import into a Streamlit dashboard for geospatial and trend visualization.
