from datetime import datetime, timedelta
import requests as rq
import pandas as pd
from time import sleep
from random import randint

import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from models import Base, WB_comission
from access_ import mpAccess, dbAccess

from wb_comission_log import log_comission


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 10, 6),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}
schedule = '@daily'

with DAG('wb_comission_logger',
         default_args=default_args,
         schedule=schedule,
         catchup=False) as dag:
    
    t1 = PythonOperator(task_id = 'log_comission', python_callable = log_comission)

    t1