from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
	ui_color = "#80BD9E"

	@apply_defaults
	def __init__(self,
				 redshift_conn_id = "",
				 table = "",
				 sql = "",
				 append = False,
				 *args, **kwargs):
		
		super(LoadDimensionOperator, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.table = table
		self.sql = sql
		self.append = append

	def execute(self, context):
		"""
		Loads data from staging to dimension table
		"""

		redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
		if not self.append:
			self.log.info(f"TRUNCATING table {self.table}...")
			redshift.run(f"TRUNCATE TABLE {self.table};")

		self.log.info(f"LOADING data into dimension {self.table}...")
		redshift.run(self.sql)