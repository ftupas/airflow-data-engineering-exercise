from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import helpers
import operators

# Defining the plugin class

class NycTlcPlugin(AirflowPlugin):
	name = "nyc_tlc_plugin"
	operators = [
		operators.CreateTablesOperator,
		operators.S3ToRedshiftOperator,
		operators.LoadDimensionOperator,
		operators.LoadFactOperator,
		operators.LoadCalcOperator
	]
	helpers = [
		helpers.SqlQueries
	]