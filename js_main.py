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
from retail_models import JSretail_clients, JSretail_sales, JSretail_shops, JSretail_sku
from retail_main_upsert import upsert_clients, upsert_sales, upsert_shops, upsert_sku


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 10, 6),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}
schedule = '@daily'

with DAG('js_main_upsert',
    default_args = default_args, 
    schedule = schedule,
    catchup=False
) as dag:
    t1 = PythonOperator(task_id = 'upsert_sku', python_callable=upsert_sku, op_kwargs = {'utAccess' : utAccess, 'dbAccess' : dbAccess})
    t2 = PythonOperator(task_id = 'upsert_shops', python_callable=upsert_shops, op_kwargs = {'utAccess' : utAccess, 'dbAccess' : dbAccess})
    t3 = PythonOperator(task_id = 'upsert_clients', python_callable=upsert_clients, op_kwargs = {'utAccess' : utAccess, 'dbAccess' : dbAccess})
    t4 = PythonOperator(task_id = 'upsert_sales', python_callable=upsert_sales, op_kwargs = {'utAccess' : utAccess, 'dbAccess' : dbAccess})

    t1
    t2
    t3
    t4