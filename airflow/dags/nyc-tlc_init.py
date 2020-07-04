from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
import datetime
import logging
import sql_queries

default_args = {
	"owner": "fred",
	"start_date": datetime.datetime(2019, 11, 1),
	"end_date": datetime.datetime(2019, 11, 1)
}

with DAG(dag_id = "nyc-tlc-init",
	default_args = default_args) as dag:
	"""
	This dag creates the tables
	"""

	def copy_lookup():
		aws_hook = AwsHook("aws_credentials")
		credentials = aws_hook.get_credentials()
		redshift_hook = PostgresHook("redshift")
		logging.info("COPYING lookup table...")
		redshift_hook.run(sql_queries.COPY_LOOKUP_SQL.format(
			credentials.access_key,
			credentials.secret_key))

	create_tables = PostgresOperator(
	task_id = "create_tables",
	dag = dag,
	postgres_conn_id = "redshift",
	sql = sql_queries.CREATE_TABLES
	)

	copy_look_up_task = PythonOperator(
		task_id = "copy_lookup",
		python_callable = copy_lookup,
		dag = dag
		)

create_tables >> copy_look_up_task

