# Databricks notebook source
# MAGIC %md
# MAGIC # API monitoring of All-purpose clusters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC _author_: **Maksim Pachkouski** - [Databricks MVP | Senior Data Engineer]
# MAGIC - [file source](https://github.com/protmaks/Databricks/blob/main/API%20%26%20SDK/Monitoring/API%20monitoring%20of%20All-purpose%20clusters.py)
# MAGIC - [description](https://medium.com/@protmaks/databricks-cost-optimization-api-monitoring-of-all-purpose-clusters-b7ad7ddd4702)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@protmaks) &nbsp;
# MAGIC [![LinkedIn](https://img.shields.io/badge/LinkedIn-3572A5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/protmaks) &nbsp;
# MAGIC [![GitHub](https://img.shields.io/github/followers/protmaks?label=Follow&style=social)](https://github.com/protmaks) &nbsp;
# MAGIC

# COMMAND ----------

# DBTITLE 1,imports
import pytz, requests, pyarrow as pa, json, pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from graphlib import TopologicalSorter
from pprint import pprint
from pyspark.sql import Row
from collections import defaultdict
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient

wc = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,parameters
report_time_zone = 'America/New_York'    # set your time zone
report_days = 10    # set the number of days

# COMMAND ----------

# DBTITLE 1,clusters
df_clusters_data = [
    Row(
        cluster_id=c.cluster_id,
        cluster_name=c.cluster_name,
        state=c.state.value if c.state else "UNKNOWN",
        creator=c.creator_user_name
    )
    for c in wc.clusters.list()
    if c.cluster_source and c.cluster_source.value == "UI"
]

spark.createDataFrame(df_clusters_data).createOrReplaceTempView("clusters")
#df_clusters_data.display()

# COMMAND ----------

# DBTITLE 1,all_clusters_events
end_time = datetime.now()
start_time = end_time - timedelta(days=report_days)
start_ms = int(start_time.timestamp() * 1000)
end_ms = int(end_time.timestamp() * 1000)

clusters = [c for c in wc.clusters.list() if c.cluster_source and c.cluster_source.value == 'UI']
print(f"All-purpose clusters: {len(clusters)}")

schema = StructType([
    StructField("cluster_id", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("type", StringType(), True),
    StructField("details", StringType(), True),
])

df_all_events_data = []

for i, cluster in enumerate(clusters, 1):
    print(f"[{i}/{len(clusters)}] {cluster.cluster_name or 'Unknown'}")
    
    try:
        events = list(wc.clusters.events(
            cluster_id=cluster.cluster_id,
            start_time=start_ms,
            end_time=end_ms
        ))
        
        for event in events:
            event_dict = event.as_dict()
            
            cleaned_dict = {
                'cluster_id': str(event_dict.get('cluster_id', '')),
                'timestamp': int(event_dict.get('timestamp', 0)),
                'type': str(event_dict.get('type').value if hasattr(event_dict.get('type'), 'value') else event_dict.get('type', '')),
                'details': json.dumps(event_dict.get('details')) if isinstance(event_dict.get('details'), dict) else str(event_dict.get('details', ''))
            }
            
            df_all_events_data.append(cleaned_dict)
        
        print(f"  → Events: {len(events)}")
    except Exception as e:
        print(f"  → Error: {str(e)[:80]}")

print(f"\nAll Events: {len(df_all_events_data)}")

if df_all_events_data:
    spark.createDataFrame(df_all_events_data, schema=schema).createOrReplaceTempView("all_clusters_events")

# COMMAND ----------

# DBTITLE 1,events_aggr
df_events_cl = spark.sql(f"""
  SELECT
    cluster_id,
    cluster_name,
    'unknown' as team,  -- add command definition logic
    event_time_utc,
    event_time,
    type,
    inactivity_duration_min,
    concat_ws(', ', collect_list(user)) as user,
    concat_ws(', ', collect_list(reason)) as reason 
  FROM( 
    SELECT
      e.cluster_id,
      c.cluster_name,
      date_trunc('minute',to_timestamp(FROM_UNIXTIME(e.timestamp / 1000))) AS event_time_utc,
      from_utc_timestamp(to_timestamp(FROM_UNIXTIME(e.timestamp / 1000)), '{report_time_zone}') as event_time,
      e.type,
      --e.details,
      CASE
        WHEN e.type = 'TERMINATING'
          THEN coalesce(GET_JSON_OBJECT(details, '$.reason.parameters.username'),'autotermination')
        WHEN e.type = 'STARTING'
          THEN GET_JSON_OBJECT(details, '$.user')
        ELSE ''
      END AS user,
      CASE
        WHEN e.type = 'TERMINATING'
          THEN concat(GET_JSON_OBJECT(details, '$.reason.code'), " ", coalesce(GET_JSON_OBJECT(details, '$.reason.parameters.inactivity_duration_min'),''))
        WHEN e.type = 'STARTING'
          THEN coalesce(GET_JSON_OBJECT(details, '$.job_run_name'), "manually")
        ELSE ''
      END AS reason,
      CASE
        WHEN e.type = 'TERMINATING'
          THEN coalesce(GET_JSON_OBJECT(details, '$.reason.parameters.inactivity_duration_min'),'0')
        ELSE '0'
      END as inactivity_duration_min
    FROM all_clusters_events as e
    LEFT JOIN clusters as c
      ON e.cluster_id = c.cluster_id
    WHERE
      e.type  in ('STARTING','TERMINATING')
    ORDER BY
      e.timestamp DESC
  )
  GROUP BY
    cluster_id,
    cluster_name,
    event_time_utc,
    event_time,
    type,
    inactivity_duration_min
""")

df_events_cl.createOrReplaceTempView("events_aggr")
#df_events_cl.display()

# COMMAND ----------

# DBTITLE 1,cluster_sessions
df_cluster_sessions = spark.sql(f"""
  WITH
  
  ordered_events AS (
    SELECT
      cluster_name,
      team,
      event_time,
      type,
      user,
      reason,
      LEAD(inactivity_duration_min) OVER (PARTITION BY cluster_name ORDER BY event_time) AS inactivity_duration_min,
      LEAD(event_time) OVER (PARTITION BY cluster_name ORDER BY event_time) AS next_event_time,
      LEAD(type) OVER (PARTITION BY cluster_name ORDER BY event_time) AS next_event_type,
      LEAD(user) OVER (PARTITION BY cluster_name ORDER BY event_time) AS next_user,
      LEAD(reason) OVER (PARTITION BY cluster_name ORDER BY event_time) AS next_reason
    FROM events_aggr
  ),
  
  base_sessions AS (
    SELECT
      cluster_name,
      team,
      CASE WHEN next_event_time is null THEN 'RUNNING' ELSE 'TERMINATED' END AS result_state,
      ROW_NUMBER() OVER (ORDER BY event_time) AS session_id,
      event_time AS start_time,
      coalesce(next_event_time, from_utc_timestamp(current_timestamp(), '{report_time_zone}')) AS end_time,
      coalesce(TIMESTAMPDIFF(MINUTE, event_time, next_event_time), TIMESTAMPDIFF(MINUTE, event_time, from_utc_timestamp(current_timestamp(), '{report_time_zone}'))) AS duration_min,
      inactivity_duration_min,
      user AS started_by,
      reason AS start_reason,
      next_user AS terminated_by,
      next_reason AS termination_reason
    FROM ordered_events
    WHERE type = 'STARTING'
  ),
  
  adjusted_sessions AS (
    SELECT
      cluster_name,
      team,
      result_state,
      session_id,
      start_time,
      CASE 
        WHEN termination_reason like '%INACTIVITY%' 
        THEN TIMESTAMPADD(MINUTE, -inactivity_duration_min, end_time)
        ELSE end_time 
      END AS end_time,
      inactivity_duration_min,
      started_by,
      start_reason,
      terminated_by,
      termination_reason
    FROM base_sessions
    
    UNION ALL
    
    SELECT
      cluster_name,
      team,
      'WAITING' AS result_state,
      session_id,
      TIMESTAMPADD(MINUTE, -inactivity_duration_min, end_time) AS start_time,
      end_time,
      inactivity_duration_min,
      started_by,
      start_reason,
      terminated_by,
      termination_reason
    FROM base_sessions
    WHERE termination_reason like '%INACTIVITY%'
  ),
  
  date_range AS (
    SELECT
      cluster_name,
      team,
      result_state,
      session_id,
      start_time,
      end_time,
      inactivity_duration_min,
      started_by,
      start_reason,
      terminated_by,
      termination_reason,
      DATE(start_time) AS start_date,
      COALESCE(DATE(end_time), CURRENT_DATE()) AS end_date,
      DATEDIFF(COALESCE(DATE(end_time), CURRENT_DATE()), DATE(start_time)) AS days_diff
    FROM adjusted_sessions
  ),
  
  expanded_dates AS (
    SELECT
      cluster_name,
      team,
      result_state,
      session_id,
      start_time,
      end_time,
      inactivity_duration_min,
      started_by,
      start_reason,
      terminated_by,
      termination_reason,
      start_date,
      end_date,
      EXPLODE(SEQUENCE(0, days_diff)) AS day_offset
    FROM date_range
  )
  
  SELECT
    cluster_name,
    team,
    result_state,
    session_id,
    DATE_ADD(start_date, day_offset) AS session_date,
    CASE 
      WHEN day_offset = 0 THEN start_time
      ELSE CAST(CONCAT(CAST(DATE_ADD(start_date, day_offset) AS STRING), ' 00:00:00') AS TIMESTAMP)
    END AS start_time,
    CASE 
      WHEN DATE_ADD(start_date, day_offset) = end_date THEN end_time
      ELSE CAST(CONCAT(CAST(DATE_ADD(start_date, day_offset) AS STRING), ' 23:59:59') AS TIMESTAMP)
    END AS end_time,
    TIMESTAMPDIFF(
      MINUTE,
      CASE 
        WHEN day_offset = 0 THEN start_time
        ELSE CAST(CONCAT(CAST(DATE_ADD(start_date, day_offset) AS STRING), ' 00:00:00') AS TIMESTAMP)
      END,
      CASE 
        WHEN DATE_ADD(start_date, day_offset) = end_date THEN end_time
        ELSE CAST(CONCAT(CAST(DATE_ADD(start_date, day_offset) AS STRING), ' 23:59:59') AS TIMESTAMP)
      END
    ) AS duration_min,
    inactivity_duration_min,
    started_by,
    start_reason,
    terminated_by,
    termination_reason
  FROM expanded_dates
  
  ORDER BY start_time DESC
""")

df_cluster_sessions.createOrReplaceTempView("cluster_sessions")
#df_cluster_sessions.display()

# COMMAND ----------

# DBTITLE 1,plot
df = df_cluster_sessions.toPandas()

df["date"] = df["start_time"].dt.date
unique_dates = sorted(df["date"].unique(), reverse=True)
unique_teams = ["All"] + sorted(df["team"].unique().tolist()) if "team" in df.columns else ["All"]

cluster_name_order = sorted(df["cluster_name"].unique().tolist())

num_clusters = len(cluster_name_order)
height_per_cluster = 50
base_height = 100
calculated_height = base_height + (num_clusters * height_per_cluster)

all_traces = []
trace_metadata = []

color_map = {
    "RUNNING": "blue",
    "CANCELED": "orange",
    "SUCCESS": "green",
    "SUCCEEDED": "green",
    "FAILED": "red",
    "PLAN": "grey",
    "TERMINATED": "black",
    "WAITING": "red"
}

for date_idx, date in enumerate(unique_dates):
    for team_idx, team in enumerate(unique_teams):
        if team == "All":
            df_filtered = df[df["date"] == date]
        else:
            df_filtered = df[(df["date"] == date) & (df["team"] == team)]
        
        if df_filtered.empty:
            continue
        
        fig_temp = px.timeline(
            df_filtered,
            x_start="start_time",
            x_end="end_time",
            y="cluster_name",
            color="result_state",
            color_discrete_map=color_map,
            category_orders={"cluster_name": cluster_name_order},
            hover_data={
                "start_time": "|%H:%M:%S",
                "end_time": "|%H:%M:%S",
                "started_by": True,
                "start_reason": True,
                "terminated_by": True,
                "termination_reason": True
            }
        )
        
        start_idx = len(all_traces)
        
        for trace in fig_temp.data:
            trace.visible = (date_idx == 0 and team == "All")
            trace.width = 0.4
            trace.marker.line.width = 0
            all_traces.append(trace)
        
        end_idx = len(all_traces)
        trace_metadata.append({
            "date": date,
            "team": team,
            "start_idx": start_idx,
            "end_idx": end_idx
        })

date_buttons = []
for date in unique_dates:
    day_start = pd.Timestamp(date).normalize()
    day_end = day_start + pd.Timedelta(days=1)
    
    visible_array = [False] * len(all_traces)
    for meta in trace_metadata:
        if meta["date"] == date and meta["team"] == "All":
            for i in range(meta["start_idx"], meta["end_idx"]):
                visible_array[i] = True
    
    date_buttons.append(
        dict(
            label=str(date),
            method="update",
            args=[
                {"visible": visible_array},
                {
                    "xaxis.range": [day_start, day_end],
                    "title.text": f"Clusters execution timeline - {date}"
                }
            ]
        )
    )

team_buttons = []
for team in unique_teams:
    team_buttons.append(
        dict(
            label=team,
            method="relayout",
            args=[{"title.text": f"Clusters execution timeline - {team}"}]
        )
    )

fig = go.Figure(data=all_traces)

fig.update_layout(
    updatemenus=[
        dict(
            buttons=date_buttons,
            direction="down",
            showactive=True,
            x=0.00,
            xanchor="left",
            y=1.65,
            yanchor="top"
        ),
        dict(
            buttons=team_buttons,
            direction="down",
            showactive=True,
            x=0.15,
            xanchor="left",
            y=1.65,
            yanchor="top"
        )
    ],
    barmode='overlay'
)

fig.update_yaxes(
    autorange="reversed",
    categoryorder='array',
    categoryarray=cluster_name_order
)

first_day_start = pd.Timestamp(unique_dates[0]).normalize()
first_day_end = first_day_start + pd.Timedelta(days=1)

fig.update_layout(
    xaxis=dict(
        range=[first_day_start, first_day_end],
        tickformat="%H:%M",
        type="date" 
    ),
    height=calculated_height,
    title=f"Clusters execution timeline"
)

fig.show()