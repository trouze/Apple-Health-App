# dash.py

from subprocess import run
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
import numpy as np
import pydeck as pdk

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache to only rerun when the query changes or after 10 min.
@st.cache(ttl=600000)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

workouts = run_query("""
SELECT
CONCAT(REGEXP_REPLACE(duration,"HKWorkoutActivityType",""), "_",FORMAT_TIMESTAMP("%Y%m%d",endDate)) AS ID,
FORMAT_TIMESTAMP("%Y%m%d%H%M",endDate) AS time_id,
durationUnit AS Duration, 
totalDistance AS DurationUnits, 
totalDistanceUnit AS Distance, 
totalEnergyBurned AS DistanceUnits,
totalEnergyBurnedUnit AS EnergyBurned,
sourceName AS EnergyBurnedUnit,
sourceVersion AS Device,
device AS OSversion,
startDate AS creationDate,
endDate AS startDate,
timestamp_field_13 AS endDate
FROM `tylers-apple-data.workout_data.workouts`
""")
df = pd.DataFrame(workouts)

# query gpx data based on selection

gpx = run_query("""
WITH t1 AS
(
    SELECT
    lon AS num,
    lat AS lon,
    speed AS lat,
    course AS speed,
    hAcc AS course,
    vAcc AS hAcc,
    name AS vAcc,
    trkpt,
    FORMAT_DATETIME("%Y%m%d%H%M",PARSE_DATETIME("%Y-%m-%d %l:%M%p",LTRIM(trkpt,'Route '))) AS name,
    time AS elevation,
    timestamp_field_10 AS time_interval
    FROM `tylers-apple-data.workout_data.routes`
),
t2 AS
(
    SELECT
    CONCAT(REGEXP_REPLACE(duration,"HKWorkoutActivityType",""), "_",FORMAT_TIMESTAMP("%Y%m%d",endDate)) AS ID,
    FORMAT_TIMESTAMP("%Y%m%d%H%M",TIMESTAMP_SUB(startDate, INTERVAL 5 HOUR)) AS time_id
    FROM `tylers-apple-data.workout_data.workouts`
)
SELECT
ID,
lon,
lat,
speed,
course,
hAcc,
vAcc,
elevation
FROM t1 LEFT JOIN t2 ON t1.name = t2.time_id
""")
gpx = pd.DataFrame(gpx)
# generate sidebar
a = st.sidebar.selectbox('Select a workout: ', df.ID)
midpoint = (np.average(gpx.loc[gpx["ID"] == a,'lat']), np.average(gpx.loc[gpx["ID"] == a,'lon']))
st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
        latitude=midpoint[0],
        longitude=midpoint[1],
        zoom=20,
    ),
        layers=[
            # pdk.Layer(
            # 'HexagonLayer',
            # data=df,
            # get_position='[lon, lat]',
            # radius=200,
            # elevation_scale=4,
            # elevation_range=[0, 1000],
            # pickable=True,
            # extruded=True,
            # ),
            pdk.Layer(
                'ScatterplotLayer',
                data=gpx.loc[gpx["ID"]==a,['lat','lon']],
                get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]',
                get_radius=200,
            ),
        ],
    )
)


#st.map(gpx.loc[gpx["ID"]==a,['lat','lon']])
# col1, col2 = st.columns(2)
# with col1:
#     st.write(df.loc[df["ID"] == a])


# with col2:
    
#     midpoint = (np.average(gpx.loc[gpx["ID"] == a,'lat']), np.average(gpx.loc[gpx["ID"] == a,'lon']))

#     st.map(gpx.loc[gpx["ID"]==a])