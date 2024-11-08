from models import Base, WB_comission
from access_ import mpAccess, dbAccess
import pandas as pd
import requests as rq
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
import sys
import time


def log_comission():

    auth_token = mpAccess.irm_token
    url = 'https://common-api.wildberries.ru/api/v1/tariffs/commission'
    response = rq.get(url=url, headers={"Authorization" : auth_token})
    log_date = datetime.today().strftime('%Y-%m-%d')
    
    if response.status_code == 200:
        df = pd.DataFrame(response.json()['report'])
    else:
        print(f'Response code is {response.status_code}. Breaking')
        exit
    
    
    df['date_reg'] = log_date

    print('Подключение к базе')
    print('hook = PostgresHook(postgres_conn_id="marketplace")')
    hook = PostgresHook(postgres_conn_id='marketplace')
    print('conn = hook.get_conn()')
    engine = hook.get_sqlalchemy_engine()
    print('Создание сессии')
    print('Session = sessionmaker(bind=conn)')
    Session = sessionmaker(bind=engine) 
    print('session = Session()')
    session = Session()

    for _, row in df.iterrows():
        df_find = session.query(WB_comission).filter_by(subjectID = str(row['subjectID']), date_reg = str(row['date_reg'])).first()

        if df_find:
            df_find.kgvpMarketplace = row['kgvpMarketplace']
            df_find.kgvpSupplier = row['kgvpSupplier']
            df_find.kgvpSupplierExpress = row['kgvpSupplierExpress']
            df_find.paidStorageKgvp = row['paidStorageKgvp']
            df_find.parentID = row['parentID']
            df_find.parentName = row['parentName']
            df_find.subjectID = row['subjectID']
            df_find.subjectName = row['subjectName']
            df_find.date_reg = row['date_reg']

        else:
            new_df = WB_comission(
                kgvpMarketplace = row['kgvpMarketplace'],
                kgvpSupplier = row['kgvpSupplier'],
                kgvpSupplierExpress = row['kgvpSupplierExpress'],
                paidStorageKgvp = row['paidStorageKgvp'],
                parentID = row['parentID'],
                parentName = row['parentName'],
                subjectID = row['subjectID'],
                subjectName = row['subjectName'],
                date_reg = row['date_reg']
            )

            session.add(new_df)
    session.commit()