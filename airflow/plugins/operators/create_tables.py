from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
	ui_color = "#F98866"

	@apply_defaults
	def __init__(self,
				 redshift_conn_id = "",
				 sql = "",
				 *args, **kwargs):

		super(CreateTablesOperator, self).__init__(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.sql = sql

	def execute(self, context):
		"""
		Creates the tables if they are not yet created
		"""
		
		redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

		self.log.info("CREATING tables if not existing...")
		redshift.run(self.sql)
		self.log.info("FINISHED creating tables...")