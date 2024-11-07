from datetime import datetime, timedelta
import requests as rq
import pandas as pd

import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from access_ import utAccess, dbAccess
from retail_models import JSretail_sales
from retail_sales_upsert import upsert_sales


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 10, 6),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}
schedule = '@daily'

with DAG('js_sales_upsert',
    default_args = default_args, 
    schedule = schedule,
    catchup=False
) as dag:

    t4 = PythonOperator(task_id = 'upsert_sales', python_callable=upsert_sales, op_kwargs = {'utAccess' : utAccess, 'dbAccess' : dbAccess})
    
    t4