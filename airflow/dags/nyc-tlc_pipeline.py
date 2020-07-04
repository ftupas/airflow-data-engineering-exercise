from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
import sql_queries
import datetime
import logging
import re

default_args = {
	"owner": "fred",
	'depends_on_past': False,
	"start_date": datetime.datetime(2019, 11, 1),
	"end_date": datetime.datetime(2019, 12, 1)
}

with DAG(dag_id = "nyc-tlc-pipeline",
	default_args = default_args,
	schedule_interval = "@monthly") as dag:

	def copy_new_files(*args, **kwargs):
		"""
		Gets the files for the month
		"""
		# Get report month and date
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		report_month = report_date.month
		report_year = report_date.year
		logging.info(f"Looking for files with month = {report_month} and year = {report_year}")

		# Get list of files
		hook = S3Hook(aws_conn_id = "aws_credentials")
		bucket = "nyc-tlc"
		prefix = "trip data"
		logging.info(f"Listing Keys from {bucket}/{prefix}")
		keys = hook.list_keys(bucket, prefix = prefix)

		# Get list of new files
		# regex = "^(trip data)/(\w+)_tripdata_(\d\d\d\d)-(\d\d).csv" all taxis
		regex = "^(trip data)/(yellow)_tripdata_(\d\d\d\d)-(\d\d).csv"

		new_files = []
		tables = []
		for key in keys:
			match = re.search(regex, key)
			if match:
				taxi_type = match.group(2)
				year = int(match.group(3))
				month = int(match.group(4))

				if report_year == year and report_month == month:
					new_files.append(key)
					tables.append(taxi_type)

		# Copy new files
		aws_hook = AwsHook("aws_credentials")
		credentials = aws_hook.get_credentials()
		redshift_hook = PostgresHook("redshift")
		for table, file in zip(tables, new_files):
			file.replace(" ", "+")
			redshift_hook.run(sql_queries.COPY_TRIPS_SQL.format(
				f"staging_{table}_trips",
				f"s3://{bucket}/{file}",
				credentials.access_key,
				credentials.secret_key
				))

		logging.info(f"New files are {new_files}")

	def load_trips_table():
		"""
		Loads the data from staging to trips
		"""
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"
		hook = PostgresHook("redshift")
		hook.run(sql_queries.LOAD_TRIPS)


	def calc_pop_destination_passengers_month(*args, **kwargs):
		"""
		Computes for the top 5 popular destinations per pickup-zone in
		terms of total passengers for the month and loads it to a table
		"""
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"
		hook = PostgresHook("redshift")
		hook.run(sql_queries.CALC_pop_destination_passengers_month.format(month))

	def calc_pop_destination_rides_month(*args, **kwargs):
		"""
		Computes for the rank of popular destinations per pickup-borough
		in terms of total rides for the month and loads it to a table
		"""
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"
		hook = PostgresHook("redshift")
		hook.run(sql_queries.CALC_pop_destination_rides_month.format(month))

	def load_popular_rides_full(*args, **kwargs):
		"""
		Computes for the rank of popular destinations per pickup-borough
		in terms of total rides for the month and loads it to a table
		"""
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"

		prev_ds = kwargs.get("prev_ds")
		prev_report_date = datetime.datetime.strptime(prev_ds, "%Y-%m-%d")
		prev_month = f"{prev_report_date.year}-{prev_report_date.month}"

		hook = PostgresHook("redshift")
		hook.run(sql_queries.Load_popular_rides_full.format(month, prev_month))

	def calc_current_pop_dest(*args, **kwargs):
		"""
		Computes the latest rankings of popular destinations for the month
		and loads it to a table
		"""
		ds = kwargs.get("ds")
		report_date = datetime.datetime.strptime(ds, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"
		hook = PostgresHook("redshift")
		hook.run(sql_queries.CALC_current_pop_dest.format(month))

	copy_new_files_task = PythonOperator(
		task_id = "copy_new_files",
		python_callable = copy_new_files,
		provide_context = True,
		dag = dag
		)
	
	load_trips_task = PostgresOperator(
		task_id = "load_trips_table",
		dag = dag,
		postgres_conn_id = "redshift",
		sql = sql_queries.LOAD_TRIPS
		)

	calc_pop_destination_passengers_month_task = PythonOperator(
		task_id = "calc_pop_destination_passengers_month",
		dag = dag,
		python_callable = calc_pop_destination_passengers_month,
		provide_context = True
		)

	calc_pop_destination_rides_month_task = PythonOperator(
		task_id = "calc_pop_destination_rides_month",
		dag = dag,
		python_callable = calc_pop_destination_rides_month,
		provide_context = True
		)

	load_popular_rides_full_task = PythonOperator(
		task_id = "load_popular_rides_full",
		dag = dag,
		python_callable = load_popular_rides_full,
		provide_context = True
		)

	calc_current_pop_dest_task = PythonOperator(
		task_id = "calc_current_pop_dest",
		dag = dag,
		python_callable = calc_current_pop_dest,
		provide_context = True
		)

copy_new_files_task >> load_trips_task
load_trips_task >> calc_pop_destination_passengers_month_task
load_trips_task >> calc_pop_destination_rides_month_task
load_trips_task	>> load_popular_rides_full_task
load_trips_task >> calc_current_pop_dest_task