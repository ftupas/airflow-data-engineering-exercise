from operators import (CreateTablesOperator, S3ToRedshiftOperator, LoadDimensionOperator, LoadFactOperator, LoadCalcOperator)
from helpers import SqlQueries
from airflow import DAG

import datetime

TAXI_REGEX = "^(trip data)/({})_tripdata_(\d\d\d\d)-(\d\d).csv"
LOOKUP_REGEX = "^(misc)/(taxi _zone_lookup.csv)"

S3_BUCKET = "nyc-tlc"
S3_PREFIX = "trip data"
S3_PREFIX_LOOKUP = "misc"

REDSHIFT = "redshift"
AWS_CREDENTIALS = "aws_credentials"

DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2019, 11, 1),
    "end_date": datetime.datetime(2019, 12, 1)
}

with DAG(dag_id="nyc-tlc-pipeline",
         default_args=DAG_DEFAULT_ARGS,
         schedule_interval="@monthly",
         max_active_runs=1
         ) as dag:
	
	start_operator = CreateTablesOperator(
		task_id = "init_tables",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		sql = SqlQueries.CREATE_TABLES
		)

	yellow_to_redshift_task = S3ToRedshiftOperator(
		task_id = "stage_yellow",
		dag = dag,
		aws_credentials = AWS_CREDENTIALS,
		redshift_conn_id = REDSHIFT,
		provide_context = True,
		bucket = S3_BUCKET,
		prefix = S3_PREFIX,
		table = "staging_yellow_trips",
		regex = TAXI_REGEX.format("yellow"),
		sql = SqlQueries.COPY_S3_SQL
		)

	lookup_to_redshift_task = S3ToRedshiftOperator(
		task_id = "stage_lookup",
		dag = dag,
		aws_credentials = AWS_CREDENTIALS,
		redshift_conn_id = REDSHIFT,
		bucket = S3_BUCKET,
		prefix = S3_PREFIX_LOOKUP,
		table = "staging_lookup_trips",
		regex = LOOKUP_REGEX,
		sql = SqlQueries.COPY_S3_SQL
		)

	load_dim_pickup_task = LoadDimensionOperator(
		task_id = "load_dim_pickup",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "DIM_pickup",
		sql = SqlQueries.LOAD_DIM_PICKUP_SQL,
		append = True
		)

	load_dim_dropoff_task = LoadDimensionOperator(
		task_id = "load_dim_dropoff",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "DIM_dropoff",
		sql = SqlQueries.LOAD_DIM_DROPOFF_SQL,
		append = True
		)

	load_fact_trips_task = LoadFactOperator(
		task_id = "load_fact_trips",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "FACT_trips",
		sql = SqlQueries.LOAD_FACT_TRIPS
		)

	calc_pop_destination_passengers_month_task = LoadCalcOperator(
		task_id = "calc_pop_destination_passengers_month",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "pop_destination_passengers_month",
		sql = SqlQueries.CALC_POP_DESTINATION_PASSENGERS_MONTH
		)

	calc_pop_destination_rides_month_task = LoadCalcOperator(
		task_id = "calc_pop_destination_rides_month",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "pop_destination_rides_month",
		sql = SqlQueries.CALC_POP_DESTINATION_RIDES_MONTH
		)

	calc_popular_rides_full_task = LoadCalcOperator(
		task_id = "calc_popular_rides_full",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "popular_rides_full",
		sql = SqlQueries.CALC_POPULAR_RIDES_FULL,
		compare = True
		)

	calc_current_pop_dest_task = LoadCalcOperator(
		task_id = "calc_current_pop_dest",
		dag = dag,
		redshift_conn_id = REDSHIFT,
		table = "cur_popular_dest",
		sql = SqlQueries.CALC_CURRENT_POP_DEST,
		append = False
		)

# Declare dependencies
start_operator >> [yellow_to_redshift_task, lookup_to_redshift_task]
lookup_to_redshift_task >> [load_dim_pickup_task, load_dim_dropoff_task]
yellow_to_redshift_task >> load_fact_trips_task
[load_dim_pickup_task, load_dim_dropoff_task] >> load_fact_trips_task
load_fact_trips_task >> [calc_pop_destination_passengers_month_task, 
						 calc_pop_destination_rides_month_task, 
						 calc_popular_rides_full_task,
						 calc_current_pop_dest_task]