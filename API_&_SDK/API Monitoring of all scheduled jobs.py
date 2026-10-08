# Databricks notebook source
# MAGIC %md
# MAGIC # API Monitoring of all scheduled jobs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC _author_: **Maksim Pachkouski** - [Databricks MVP | Senior Data Engineer]
# MAGIC - [file source](https://github.com/protmaks/Databricks/blob/main/API%20%26%20SDK/Monitoring/API%20Monitoring%20of%20all%20scheduled%20jobs.py)
# MAGIC - [description](https://medium.com/@protmaks/api-monitoring-of-scheduled-jobs-33a221d9f891)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@protmaks) &nbsp;
# MAGIC [![LinkedIn](https://img.shields.io/badge/LinkedIn-3572A5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/protmaks) &nbsp;
# MAGIC [![GitHub](https://img.shields.io/github/followers/protmaks?label=Follow&style=social)](https://github.com/protmaks) &nbsp;

# COMMAND ----------

# DBTITLE 1,imports
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots
import pytz, requests, pyarrow as pa
from databricks.sdk import WorkspaceClient
from graphlib import TopologicalSorter
from pprint import pprint
from pyspark.sql import Row
from collections import defaultdict
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np

report_time_zone = 'America/New_York' # set your time zone

wc = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,jobs
jobs = list(wc.jobs.list())
job_details = {j.job_id: wc.jobs.get(j.job_id) for j in jobs}

df_jr_jobs = spark.createDataFrame([
    Row(
        job_id = str(job_detail.job_id) if job_detail.job_id is not None else "",
        job_name = str(settings.name) if settings.name else "",
        job_schedule_status = (job_detail.settings.schedule.pause_status.name if job_detail.settings and job_detail.settings.schedule and job_detail.settings.schedule.pause_status else "NOT SCHEDULED"),
        job_description = str(settings.description) if settings.description else "",
        job_creator_user_name = str(job_detail.creator_user_name) if job_detail.creator_user_name else "",
        job_created_time = datetime.fromtimestamp(job_detail.created_time / 1000, tz=timezone.utc) if job_detail.created_time else None,
        job_run_as_user_name = str(settings.run_as.user_name) if settings.run_as and settings.run_as.user_name else "",
        job_clusters = str(settings.job_clusters) if hasattr(settings, "job_clusters") and settings.job_clusters else "[]",
        job_parameters = str(settings.parameters) if hasattr(settings, "parameters") and settings.parameters else "[]",
        job_timeout_seconds = str(settings.timeout_seconds) if hasattr(settings, "timeout_seconds") and settings.timeout_seconds else "0",
        job_email_on_failure = str(getattr(settings.email_notifications, "on_failure", []) or []),
        job_timeout_job_health = str(next((rule.value for rule in (settings.health.rules if settings.health and settings.health.rules else []) if getattr(rule.metric, "name", None) == "RUN_DURATION_SECONDS"), "")), 
        job_email_on_duration_warning_threshold_exceeded = str(getattr(settings.email_notifications, "on_duration_warning_threshold_exceeded", []) or []),
        job_email_on_success = str(getattr(settings.email_notifications, "on_success", []) or []),
        job_email_on_start = str(getattr(settings.email_notifications, "on_start", []) or []),
        job_email_on_streaming_backlog_exceeded = str(getattr(settings.email_notifications, "on_streaming_backlog_exceeded", []) or []),
        job_email_no_alert_for_skipped_runs = str(getattr(settings.email_notifications, "no_alert_for_skipped_runs", []) or []),
        job_max_concurrent_runs = str(settings.max_concurrent_runs) if hasattr(settings, "max_concurrent_runs") and settings.max_concurrent_runs else "1",
        job_tags = str((settings.tags or {})),
        job_trigger = str(job_detail.settings.trigger or ""),
        job_timezone = str(getattr(job_detail.settings.schedule, "timezone_id", "") or ""),
        job_cron_expression = str(getattr(job_detail.settings.schedule, "quartz_cron_expression", "") or "")
    )
    for job_id, job_detail in job_details.items()
    for settings in [job_detail.settings]
])

df_jr_jobs = (
    df_jr_jobs
    .withColumn("job_cron_parts", split(col("job_cron_expression"), " "))
    .withColumn("job_second",       when(size(col("job_cron_parts")) == 6, lit("0")).otherwise(""))
    .withColumn("job_minute",       when(size(col("job_cron_parts")) == 6, col("job_cron_parts")[1]).otherwise(""))
    .withColumn("job_hour",         when(size(col("job_cron_parts")) == 6, col("job_cron_parts")[2]).otherwise(""))
    .withColumn("job_day_of_month", when(size(col("job_cron_parts")) == 6, col("job_cron_parts")[3]).otherwise(""))
    .withColumn("job_month",        when(size(col("job_cron_parts")) == 6, col("job_cron_parts")[4]).otherwise(""))
    .withColumn("job_day_of_week",  when(size(col("job_cron_parts")) == 6, col("job_cron_parts")[5]).otherwise(""))
    .drop("job_cron_parts")
    .withColumn("schedule_frequency", expr("""
        CASE
            WHEN job_day_of_week = '' OR job_day_of_month = ''
                THEN ''
            WHEN job_day_of_week NOT IN ('*', '?')
                THEN 'WEEKLY'
            WHEN job_day_of_month NOT IN ('*', '?')
                THEN 'MONTHLY'
            ELSE 'DAYLY' 
        END
    """))
)

df_jr_jobs.createOrReplaceTempView(f"jr_jobs")
#df_jr_jobs.display()

# COMMAND ----------

# DBTITLE 1,plan_time and next_plan_time
df_jr_jobs = (
    df_jr_jobs
    .withColumn("plan_time", expr(f"""
        CASE
            WHEN job_cron_expression <> '' THEN 
                from_utc_timestamp(
                    to_utc_timestamp(
                        make_timestamp(
                            year(current_timestamp()),
                            month(current_timestamp()),
                            day(current_timestamp()),
                            CAST(job_hour AS INT),
                            CAST(job_minute AS INT),
                            CAST(job_second AS INT)
                        ),job_timezone
                    ),
                    '{report_time_zone}'
                )
            ELSE null 
        END
    """))
    .withColumn("next_plan_time", expr(f"""
        CASE
            WHEN job_cron_expression <> '' THEN 
                from_utc_timestamp(
                    to_utc_timestamp(
                        make_timestamp(
                            year(current_timestamp()),
                            month(current_timestamp()),
                            day(current_timestamp()) + 1,
                            CAST(job_hour AS INT),
                            CAST(job_minute AS INT),
                            CAST(job_second AS INT)
                        ),job_timezone
                    ),
                    '{report_time_zone}'
                )
            ELSE null 
        END
    """))
)

df_jr_jobs.createOrReplaceTempView(f"jr_jobs")
#df_jr_jobs.display()

# COMMAND ----------

# DBTITLE 1,teams
df_jr_jobs = df_jr_jobs.withColumn("team", expr("""
        CASE
            WHEN
                job_name like '%(5 min)%'
                    THEN 'team 1'
            WHEN
                job_name like '%(10 min)%'
                    THEN 'team 2'
            ELSE 'Unknown'
        END
"""))

df_jr_jobs.createOrReplaceTempView(f"jr_jobs")
#df_jr_jobs.display()

# COMMAND ----------

# DBTITLE 1,jr_job_runs
jobs_runs = list(wc.jobs.list_runs())
df_jr_job_runs = spark.createDataFrame([
    Row(
        job_id = str(job.job_id),
        job_run_id = str(run.run_id),
        job_start_time = datetime.fromtimestamp(run.start_time / 1000, tz=timezone.utc) if run.start_time else None,
        job_end_time = datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc) if run.end_time else None,
        job_trigger_type = str(run.trigger.name or ""),
        job_run_type = str(run.run_type.name or ""),
        job_run_name = str(run.run_name or ""),
        job_parameters = str(run.overriding_parameters if hasattr(run, "overriding_parameters") else {}),
        job_result_state = str(run.state.result_state.name if run.state and run.state.result_state else "RUNNING"),
        job_termination_code = str(run.state.life_cycle_state.name if run.state and run.state.life_cycle_state else ""),
        #job_error_description = str(run.status.termination_details.type.name or ""),
        job_run_page_url = str(run.run_page_url or "")
    )
    for job in wc.jobs.list()
    for run in wc.jobs.list_runs(job_id=job.job_id)
])

df_jr_job_runs = df_jr_job_runs.withColumn("job_start_time", 
    expr(f"date_trunc('minute', from_utc_timestamp(job_start_time, '{report_time_zone}'))"))

df_jr_job_runs = df_jr_job_runs.withColumn("job_end_time", 
    expr(f"date_trunc('minute', from_utc_timestamp(job_end_time, '{report_time_zone}'))"))

df_jr_job_runs.createOrReplaceTempView(f"jr_job_runs")
#df_jr_job_runs.display()

# COMMAND ----------

# DBTITLE 1,UNION ALL
df_spark = spark.sql(f"""
WITH

last_run_duration AS (
    SELECT 
        job_id,
        unix_timestamp(job_end_time) - unix_timestamp(job_start_time) AS duration_seconds,
        ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_start_time DESC) AS rn
    FROM jr_job_runs
    WHERE job_result_state = 'SUCCEEDED' 
        AND job_end_time IS NOT NULL
),

last_run_duration_filtered AS (
    SELECT job_id, duration_seconds
    FROM last_run_duration
    WHERE rn = 1
),

actual_runs AS (
    SELECT
        j.job_id,
        j.job_name AS job,
        j.schedule_frequency,
        j.team,
        to_date(r.job_start_time) AS date,
        r.job_start_time AS start_time,
        null AS plan_time,
        r.job_result_state AS result_state,
        COALESCE(r.job_end_time, from_utc_timestamp(current_timestamp(), '{report_time_zone}')) AS end_time
    FROM jr_jobs AS j
    LEFT JOIN jr_job_runs AS r
        ON j.job_id = r.job_id
    WHERE r.job_start_time >= date_trunc('DAY', current_date() - INTERVAL 30 DAYS)
),

planned_runs AS (
    SELECT
        j.job_id,
        j.job_name AS job,
        j.schedule_frequency,
        j.team,
        to_date(j.plan_time) AS date,
        j.plan_time AS start_time,
        j.plan_time AS plan_time,
        'PLAN' AS result_state,
        TIMESTAMP_SECONDS(unix_timestamp(j.plan_time) + COALESCE(d.duration_seconds, 1800)) AS end_time
    FROM jr_jobs AS j
    LEFT JOIN last_run_duration_filtered AS d 
        ON j.job_id = d.job_id
    WHERE
        j.job_schedule_status = 'UNPAUSED'
        AND j.plan_time IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 
            FROM jr_job_runs AS r
            WHERE r.job_id = j.job_id
                AND r.job_start_time = j.plan_time
        )
),

next_planned_runs AS (
    SELECT
        j.job_id,
        --j.team AS job,
        j.job_name AS job,
        j.schedule_frequency,
        j.team,
        to_date(j.next_plan_time) AS date,
        j.next_plan_time AS start_time,
        j.next_plan_time AS plan_time,
        'PLAN' AS result_state,
        TIMESTAMP_SECONDS(unix_timestamp(j.next_plan_time) + COALESCE(d.duration_seconds, 1800)) AS end_time
    FROM jr_jobs AS j
    LEFT JOIN last_run_duration_filtered AS d 
        ON j.job_id = d.job_id
    WHERE
        j.job_schedule_status = 'UNPAUSED'
        AND j.next_plan_time IS NOT NULL
)

SELECT * FROM actual_runs

UNION ALL

SELECT * FROM planned_runs

UNION ALL

SELECT * FROM next_planned_runs
""")

#df_spark.display()

# COMMAND ----------

# DBTITLE 1,show
df = df_spark.toPandas()

df["duration_min"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
df["end_time_vis"] = df.apply(lambda r: r["start_time"] + pd.Timedelta(minutes=1) if r["duration_min"] < 1 else r["end_time"], axis=1)

df["date"] = df["start_time"].dt.date
df = df.sort_values(by=["date", "plan_time"])

unique_dates = sorted(df["date"].unique(), reverse=True)
unique_teams = ["All"] + sorted(df["team"].unique().tolist()) if "team" in df.columns else ["All"]

job_order = df.sort_values("plan_time")["job"].unique().tolist()

def calculate_concurrent_jobs(df_filtered, day_start, day_end, interval_minutes=5):
    time_points = pd.date_range(start=day_start, end=day_end, freq=f'{interval_minutes}min')
    concurrent_counts = []
    
    for time_point in time_points:
        count = ((df_filtered["start_time"] <= time_point) & 
                 (df_filtered["end_time"] > time_point)).sum()
        concurrent_counts.append(count)
    
    return time_points, concurrent_counts

all_traces = []
trace_metadata = []
max_concurrent_jobs = 0

color_map = {
    "RUNNING": "blue",
    "CANCELED": "orange",
    "SUCCESS": "green",
    "SUCCEEDED": "green",
    "FAILED": "red",
    "PLAN": "grey"
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
            x_end="end_time_vis",
            y="job",
            color="result_state",
            color_discrete_map=color_map,
            category_orders={"job": job_order}
        )
        
        start_idx = len(all_traces)
        
        for trace in fig_temp.data:
            trace.visible = (date_idx == 0 and team == "All")
            trace.width = 0.4
            trace.marker.line.width = 0
            trace.xaxis = "x"
            trace.yaxis = "y"
            all_traces.append(trace)
        
        day_start = pd.Timestamp(date).normalize()
        day_end = day_start + pd.Timedelta(days=1)
        time_points, concurrent_counts = calculate_concurrent_jobs(df_filtered, day_start, day_end)
        
        if len(concurrent_counts) > 0:
            current_max = np.max(concurrent_counts)
            if current_max > max_concurrent_jobs:
                max_concurrent_jobs = current_max
        
        concurrent_trace = go.Scatter(
            x=time_points,
            y=concurrent_counts,
            mode='lines',
            fill='tozeroy',
            name='Concurrent Jobs',
            line=dict(color='rgba(100, 149, 237, 0.8)', width=2),
            fillcolor='rgba(100, 149, 237, 0.3)',
            visible=(date_idx == 0 and team == "All"),
            xaxis="x2",
            yaxis="y2",
            showlegend=False
        )
        all_traces.append(concurrent_trace)
        
        end_idx = len(all_traces)
        trace_metadata.append({
            "date": date,
            "team": team,
            "start_idx": start_idx,
            "end_idx": end_idx
        })

def get_visible_array(selected_date, selected_team):
    visible_array = [False] * len(all_traces)
    for meta in trace_metadata:
        if meta["date"] == selected_date and meta["team"] == selected_team:
            for i in range(meta["start_idx"], meta["end_idx"]):
                visible_array[i] = True
    return visible_array

date_buttons = []
for date in unique_dates:
    day_start = pd.Timestamp(date).normalize()
    day_end = day_start + pd.Timedelta(days=1)
    
    team_buttons_for_date = []
    for team in unique_teams:
        visible_array = get_visible_array(date, team)
        team_buttons_for_date.append(
            dict(
                label=team,
                method="update",
                args=[
                    {"visible": visible_array},
                    {"title.text": f"Jobs execution timeline - {date} - {team}"}
                ]
            )
        )
    
    visible_array = get_visible_array(date, "All")
    date_buttons.append(
        dict(
            label=str(date),
            method="update",
            args=[
                {"visible": visible_array},
                {
                    "xaxis.range": [day_start, day_end],
                    "xaxis2.range": [day_start, day_end],
                    "title.text": f"Jobs execution timeline - {date} - All",
                    "updatemenus[1].buttons": team_buttons_for_date
                }
            ]
        )
    )

initial_team_buttons = []
first_date = unique_dates[0]
for team in unique_teams:
    visible_array = get_visible_array(first_date, team)
    initial_team_buttons.append(
        dict(
            label=team,
            method="update",
            args=[
                {"visible": visible_array},
                {"title.text": f"Jobs execution timeline - {first_date} - {team}"}
            ]
        )
    )

num_jobs = len(job_order)
height_per_job = 30
height_per_concurrent = 50
min_timeline_height = 200
min_concurrent_height = 150

timeline_height = np.maximum(min_timeline_height, num_jobs * height_per_job)
concurrent_height = np.maximum(min_concurrent_height, max_concurrent_jobs * height_per_concurrent)

total_height = timeline_height + concurrent_height + 100

timeline_ratio = timeline_height / (timeline_height + concurrent_height)
concurrent_ratio = concurrent_height / (timeline_height + concurrent_height)

fig = make_subplots(
    rows=2, cols=1,
    row_heights=[timeline_ratio, concurrent_ratio],
    vertical_spacing=0.08,
    subplot_titles=("Timeline", "Concurrent Jobs (5 min intervals)")
)

for trace in all_traces:
    if trace.yaxis == "y2":
        fig.add_trace(trace, row=2, col=1)
    else:
        fig.add_trace(trace, row=1, col=1)

fig.update_layout(
    updatemenus=[
        dict(
            buttons=date_buttons,
            direction="down",
            showactive=True,
            x=0.30,
            xanchor="left",
            y=1.15,
            yanchor="top"
        ),
        dict(
            buttons=initial_team_buttons,
            direction="down",
            showactive=True,
            x=0.45,
            xanchor="left",
            y=1.15,
            yanchor="top"
        )
    ],
    barmode='overlay',
    height=int(total_height),
    title=f"Jobs execution timeline"
)

first_day_start = pd.Timestamp(unique_dates[0]).normalize()
first_day_end = first_day_start + pd.Timedelta(days=1)

fig.update_xaxes(range=[first_day_start, first_day_end], tickformat="%H:%M", type="date", row=1, col=1)
fig.update_xaxes(range=[first_day_start, first_day_end], tickformat="%H:%M", type="date", row=2, col=1)
fig.update_yaxes(autorange="reversed", categoryorder='array', categoryarray=job_order, row=1, col=1)
fig.update_yaxes(title_text="Count", row=2, col=1)

fig.show()