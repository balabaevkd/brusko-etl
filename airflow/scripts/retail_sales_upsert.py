import requests as rq
import pandas as pd
from access_ import utAccess, dbAccess
import sys
import datetime

import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

import re
from retail_models import Base, JSretail_items, JSretail_checks

def get_actual_check_guids(dbAccess):  
    
    engine = db.create_engine('postgresql+psycopg2://admin:Gidras7Bibikaz@192.168.188.73:5438/js_db')
    conn = engine.connect()
    print('Получение актуальных чеков')
    checks_select = select(JSretail_checks.check_guid)

    data = conn.execute(checks_select).all()
    print('Формирование dataframe')
    data = pd.DataFrame(data)
    if data.shape == (0,0):
        print('В базе пусто. Делаю подложный датафрейм')
        data = pd.DataFrame({'check_guid' : [], 'datetime' : [], 'shop_guid' : [], 'client_guid' : [], 'sku_guid' : [], 'pcs' : [], 'price' : []})
        conn.close()
        print('Соединение закрыто, возвращаю подложный датафрейм')
        return data
    else:
        conn.close()
        print('Соединение закрыто, возвращаю актуальный датафрейм')
        return data
    




def filter_guids(existing_guids, new_guids):
    return new_guids[~new_guids['check_guid'].isin(existing_guids['check_guid'])]

def upsert_sales(utAccess, dbAccess):

    access = utAccess()
    login = access.retail_login
    password = access.retail_pass
    host = access.host
    url_sales = f'https://{login}:{password}@{host}/hs/discharge/sales/'

    param_date_to = (datetime.datetime.now() + datetime.timedelta(days= 1)).strftime('%Y%m%d%H%M%S')
    
    existing_guids = pd.DataFrame(get_actual_check_guids(dbAccess))
    print('sending GET')
    new_guids = rq.get(url=url_sales, verify=False, params={'date_from' : '20241001', 'date_to' : param_date_to})
    print('checnking GET')
    if new_guids.status_code == 200:
        new_guids = pd.DataFrame(new_guids.json())
        print('making dataframe')
    else:
        print(new_guids.status_code)
        print('not 200 exit')
        sys.exit('Not 200')

    # print('cleaning data')
    # for i in sales.columns:
    #     sales[i] = sales[i].apply(lambda x: retail_data_cleaner(x))
    new_guids_filtered = filter_guids(existing_guids, new_guids)
    to_checks = new_guids_filtered[['check_guid', 'datetime', 'shop_guid', 'client_guid']].drop_duplicates()
    to_items = new_guids_filtered[['check_guid', 'sku_guid', 'pcs', 'price']]
    to_checks['datetime'] = pd.to_datetime(to_checks['datetime'], format='%d.%m.%Y %H:%M:%S')

    print('making connection')
    hook = PostgresHook(postgres_conn_id='js_db')
    engine = hook.get_sqlalchemy_engine()
    print('engine declared')
    Session = sessionmaker(bind=engine)
    print('session made')
    session = Session()
    print('sessioin declared')
    # Вот тут обрубается
    print('after comment')

    # Upsert checks
    for _, row in to_checks.iterrows():
        print('im in iteration')
        df_find = session.query(JSretail_checks).filter_by(check_guid=row['check_guid']).first()

        if df_find:
            df_find.datetime = row['datetime']
            df_find.check_guid = row['check_guid']
            df_find.shop_guid = row['shop_guid']
            df_find.client_guid = row['client_guid']

        else:
            new_df = JSretail_checks(
                datetime = row['datetime'],
                check_guid = row['check_guid'],
                shop_guid = row['shop_guid'],
                client_guid = row['client_guid']
            )
            session.add(new_df)
        print('Commit row')    
        session.commit()
    # Upsert items


    print('Commit items')
    for _, row in to_items.iterrows():
        items = JSretail_items(
            check_guid = row['check_guid'],
            sku_guid = row['sku_guid'],
            pcs = row['pcs'],
            price = row['price']
        )
        session.add(items)

    session.commit()

