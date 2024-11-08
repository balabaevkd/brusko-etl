import pandas as pd
import datetime
import requests as rq
import time
import numpy as np

from models import Base, OZON_conversion

from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
import sys
from access_ import mpAccess


def transform_dataframe(df, metrics):
 # Создаем пустой список для хранения данных
 data = []

 # Проходим по строкам исходного DataFrame
 for index, row in df.iterrows():
  # Извлекаем значения из строки
  product_data = row[0]
  values = row[1]
  timestamp = row[2]

  # Создаем словарь с данными для новой строки
  new_row = {
   **product_data[0], # Распаковываем словарь из product_data
   'timestamp': timestamp
  }

  # Добавляем значения метрик в словарь
  for i, metric in enumerate(metrics):
   new_row[metric] = values[i]

  # Добавляем новую строку в список
  data.append(new_row)

 # Создаем новый DataFrame из списка данных
 new_df = pd.DataFrame(data)
 new_df = new_df.rename(columns={'id' : 'ozon_id'})

 return new_df

def upsert_ozon_conversion():
    today = str(datetime.datetime.today().date())
    metrics = ['hits_view_search', 'hits_view_pdp', 'hits_tocart_search', 'hits_tocart_pdp', 'ordered_units', 'position_category']
    
    seller_list = [mpAccess.geroy, mpAccess.sexologic]

    for creds in seller_list: 
        print('START NEW CREDENTIALS')
        response = rq.post('https://api-seller.ozon.ru/v1/analytics/data', headers=creds,
                    json={
                        'date_from' : today,
                        'date_to' : today,
                        'dimension' : ['sku'],
                        'metrics' : metrics,
                        'limit' : 1000})

        unparsed = response.json()

        df = pd.DataFrame(unparsed['result']['data'])
        df['timestamp'] = unparsed['timestamp']

        df = transform_dataframe(df, metrics)
        df = df.replace(np.nan, None)

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
            
            df_find = session.query(OZON_conversion).filter_by(ozon_id = str(row['ozon_id']), timestamp = str(row['timestamp'])).first()

            if df_find:
                df_find.ozon_id = row['ozon_id']
                df_find.name = row['name']
                df_find.timestamp = row['timestamp']
                df_find.hits_view_search = row['hits_view_search']
                df_find.hits_view_pdp = row['hits_view_pdp']
                df_find.hits_tocart_search = row['hits_tocart_search']
                df_find.hits_tocart_pdp = row['hits_tocart_pdp']
                df_find.ordered_units = row['ordered_units']
                df_find.position_category = row['position_category']
            else:
                new_df = OZON_conversion(
                        ozon_id = row['ozon_id'],
                        name = row['name'],
                        timestamp = row['timestamp'],
                        hits_view_search = row['hits_view_search'],
                        hits_view_pdp = row['hits_view_pdp'],
                        hits_tocart_search = row['hits_tocart_search'],
                        hits_tocart_pdp = row['hits_tocart_pdp'],
                        ordered_units = row['ordered_units'],
                        position_category = row['position_category']   
                )
                session.add(new_df)
            session.commit()

