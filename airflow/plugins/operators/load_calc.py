from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import datetime

class LoadCalcOperator(BaseOperator):
	ui_color = "#A1972D"

	@apply_defaults
	def __init__(self,
				 redshift_conn_id = "",
				 table = "",
				 sql = "",
				 append = True,
				 compare = False,
				 *args, **kwargs):

		super(LoadCalcOperator, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.table = table
		self.sql = sql
		self.append = append
		self.compare = compare

	def execute(self, context):
		"""
		Load data from fact to calculated table
		"""

		timestamp = context.get("ds")
		report_date = datetime.datetime.strptime(timestamp, "%Y-%m-%d")
		month = f"{report_date.year}-{report_date.month}"
		redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

		if not self.append:
			self.log.info(f"TRUNCATING TABLE {self.table}...")
			redshift.run(f"TRUNCATE TABLE {self.table};")

		self.log.info(f"LOADING data into {self.table}...")

		if self.compare:
			previous_timestamp = context.get("prev_ds")
			prev_report_date = datetime.datetime.strptime(previous_timestamp, "%Y-%m-%d")
			prev_month = f"{prev_report_date.year}-{prev_report_date.month}"
			redshift.run(self.sql.format(month, prev_month))

		else:
			redshift.run(self.sql.format(month))