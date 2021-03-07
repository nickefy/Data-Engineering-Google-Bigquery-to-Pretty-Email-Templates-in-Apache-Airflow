from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from os import environ
from google.cloud import bigquery
from datetime import datetime
from datetime import timedelta
from airflow import configuration
from airflow.utils.email import send_email
import time
import logging
import pandas as pd
import numpy as np
import pandas_gbq
from pretty_html_table import build_table


class BigqueryToEmail(BaseOperator):
	"""
	Send email to inform whether a process is completed or aborted
	"""

	def __init__(
		self, 
		query,
		receivers,
		table_theme,
		*args, **kwargs):

		super(SendReviewStats, self).__init__(*args, **kwargs)
		self.query = query
		self.receivers = receivers
		self.table_theme = table_theme

	def __bigquery_to_email(self, execution_date):

		client = bigquery.Client()

		# Extracting data from Bigquery
		query = self.sql.replace("$EXECUTION_DATE", "'" + execution_date + "'")
		project_id = 'Bigquery Project Id'
		df = pandas_gbq.read_gbq(query, project_id=project_id)

		# Transforming Pandas Dataframe into HTML Table
		output_table = build_table(df, self.table_theme)

		# Preparing Email Content
		html = '<!DOCTYPE html><html><p style="font-family:verdana">'
		html += """Here's the data for the day: """
		html += '<hr>'
		html += output_table
		html += '</p></html>'

		#defining receivers and subject
		email_subject = 'Sample Data from Bigquery'
		email_to = self.receivers
		email_content = html
		send_email(email_to, email_subject, email_content)
		
	def execute(self, context):
		execution_date = (context.get('execution_date')).strftime('%Y-%m-%d')
		self.__bigquery_to_email(execution_date)
