from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars

import re
import datetime

class S3ToRedshiftOperator(BaseOperator):
    ui_color = "#8D909B"

    @apply_defaults
    def __init__(self,
    			 aws_credentials = "",
    			 redshift_conn_id = "",
    			 bucket = "",
    			 prefix = "",
    			 table = "",
    			 regex = "",
    			 sql = "",
    			 append = False,
    			 *args, **kwargs):

    	super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
    	self.aws_credentials = aws_credentials
    	self.redshift_conn_id = redshift_conn_id
    	self.bucket = bucket
    	self.prefix = prefix
    	self.table = table
    	self.regex = regex.format(table)
    	self.sql = sql
    	self.append = append

    def execute(self, context):
    	"""
    	Copies the new files for the month from S3
    	"""
    	
    	timestamp = context.get("ds")
    	report_date = datetime.datetime.strptime(timestamp, "%Y-%m-%d")
    	report_month, report_year = report_date.month, report_date.year
    	self.log.info(f"Looking for files with month = {report_month} and year = {report_year}")
    	
    	# Get list files
    	s3_hook = S3Hook(aws_conn_id = self.aws_credentials)
    	self.log.info(f"Listing Keys from {self.bucket}/{self.prefix}")
    	keys = s3_hook.list_keys(self.bucket, prefix=self.prefix)

    	# Get list of new files
    	new_files = []
    	for key in keys:
    		expr = re.compile(self.regex)
    		match = expr.match(key)
    		
    		if match:
    			if match.group(1) == "misc":
    				new_files.append(key)
    			else:
    				year = int(match.group(3))
    				month = int(match.group(4))
    				if report_year == year and report_month == month:
    					new_files.append(key)
        
        # Copy new files
    	aws_hook = AwsHook(self.aws_credentials)
    	credentials = aws_hook.get_credentials()
    	redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

    	for file in new_files:
        	file.replace(" ", "+")

        	if not self.append:
        		self.log.info(f"TRUNCATING table {self.table}...")
        		redshift.run(f"TRUNCATE TABLE {self.table};")

        	self.log.info(f"COPYING table {self.table}...")
        	redshift.run(self.sql.format(
        		self.table,
                f"s3://{self.bucket}/{file}",
				credentials.access_key,
				credentials.secret_key))
        	self.log.info(f"COPIED table {self.table}.")