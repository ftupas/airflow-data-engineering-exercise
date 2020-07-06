from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
	ui_color = "#36494E"

	@apply_defaults
	def __init__(self,
				 redshift_conn_id = "",
				 table = "",
				 sql = "",
				 *args, **kwargs):

		super(LoadFactOperator, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.table = table
		self.sql = sql

	def execute(self, context):
		"""
		Loads data from staging to fact table
		"""

		redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

		self.log.info(f"LOADING data into fact table {self.table}...")
		redshift.run(self.sql)