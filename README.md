# Apple-Health-App
End-to-end platform that picks up Apple Health export data from iCloud and runs a data pipeline via [Prefect](https://www.prefect.io/) to monitor data engineering tasks as they run onboard a machine running a Prefect Agent. Posts data to a BigQuery table for import into a Streamlit dashboard for geospatial and trend visualization.

# Development Notes to me
Log the process followed for this project as well as learnings in way of Prefect, Linux, Cloud.

To keep a server process running while outside of SSH
Use nohup:
nohup prefect agent local start &
Then type:
exit
You might not actually need nohup but you definitely need the &

Start process in the current working directory

ps will list running processes