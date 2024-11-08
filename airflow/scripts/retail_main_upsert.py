import requests as rq
import pandas as pd
from access_ import utAccess, dbAccess
import sys
import datetime

import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook

from retail_models import JSretail_clients, JSretail_sales, JSretail_shops, JSretail_sku
from retail_sku_classifier import classify_macro, classify_sku, cts


access = utAccess()
login = access.retail_login
password = access.retail_pass
host = access.host


def replace_invalid_utf8(text, replacement_char="?"):

  try:
    text.encode('utf-8') 
    return text 
  except UnicodeEncodeError:
    return ''.join(
        [char if ord(char) < 128 else replacement_char for char in text]
    )

def replace_nulls(x):
    if '-' not in x:
        return None
    else: return x

# Sales

def upsert_sales(utAccess, dbAccess):

    access = utAccess()
    login = access.retail_login
    password = access.retail_pass
    host = access.host
    url_sales = f'https://{login}:{password}@{host}/hs/discharge/sales/'

    param_date_to = datetime.datetime.today().strftime('%Y%m%d%H%M%S')
    
    print('sending GET')
    sales = rq.get(url=url_sales, verify=False, params={'date_from' : '20241016110000', 'date_to' : '20241017110000'})
    print('checnking GET')
    if sales.status_code == 200:
        sales = pd.DataFrame(sales.json())
        print('making dataframe')
    else:
        print(sales.status_code)
        print('not 200 exit')
        sys.exit('Not 200')
    print('making connection')
    hook = PostgresHook(postgres_conn_id='js_db')
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    print('start iteration')

    #sales = sales.drop(['datetime', 'pcs', 'price', 'sku_guid', 'shop_guid', 'check_guid'], axis=1)
    for _, row in sales.iterrows():
        
        df_find = session.query(JSretail_sales).filter_by(id=str(row['id'])).first()

        if df_find:
            df_find.id = row['id']
            df_find.datetime = row['datetime']
            df_find.check_guid = row['check_guid']
            df_find.shop_guid = row['shop_guid']
            df_find.sku_guid = row['sku_guid']
            df_find.client_guid = row['check_guid']
            df_find.pcs = row['pcs']
            df_find.price = row['price']

        else:
            new_df = JSretail_sales(
            id = row['id'],
            datetime = row['datetime'],
            check_guid = row['check_guid'],
            shop_guid = row['shop_guid'],
            sku_guid = row['sku_guid'],
            client_guid = row['client_guid'],
            pcs = row['pcs'],
            price = row['price']
            
        )
            session.add(new_df)
        
        session.commit()


def say_hello():
    print('hello world')

def upsert_clients(utAccess, dbAccess):
    access = utAccess()
    login = access.retail_login
    password = access.retail_pass
    host = access.host
    # Part to edit
    url_clients = f'https://{login}:{password}@{host}/hs/discharge/clients/'
    print('making requests')
    clients = rq.get(url=url_clients, verify=False)

    if clients.status_code == 200:
        print(clients.status_code)
        print('init dataframe')
        clients = pd.DataFrame(clients.json(encoding = 'utf-8'))
    else:
        print(clients.status_code)
        sys.exit('Not 200')
    # End part
    print('init connection')

    clients['name'] = clients['name'].apply(lambda x: replace_nulls(x))
    clients['birth_date'] = clients['birth_date'].apply(lambda x: replace_nulls(x))
    clients['reg_date'] = clients['reg_date'].apply(lambda x: replace_nulls(x))

    hook = PostgresHook(postgres_conn_id='js_db', encoding='utf-8')
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print('start upsert iteration')
    for _, row in clients.iterrows():
        df_find = session.query(JSretail_clients).filter_by(client_guid=str(row['client_guid'])).first()

        if df_find:
            df_find.client_guid = row['client_guid']
            df_find.name = row['name']
            #df_find.birth_date = row['birth_date']
            df_find.reg_date = row['reg_date']            


        else:
            new_df = JSretail_clients( 
            client_guid = row['client_guid'],
            name = row['name'],
            #birth_date = row['birth_date'],
            reg_date = row['reg_date']
            
        )
            session.add(new_df)
    session.commit()

# Shops
def upsert_shops(utAccess, dbAccess):
    access = utAccess()
    login = access.retail_login
    password = access.retail_pass
    host = access.host
    # Part to edit
    url_shops = f'https://{login}:{password}@{host}/hs/discharge/shops/'
    shops = rq.get(url=url_shops, verify=False)

    if shops.status_code == 200:
        shops = pd.DataFrame(shops.json())
    else:
        sys.exit('Not 200')
    # End part
    hook = PostgresHook(postgres_conn_id='js_db')
    #engine = db.create_engine('postgresql+psycopg2://тут_доступы/js_db')
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()


    for _, row in shops.iterrows():
        df_find = session.query(JSretail_shops).filter_by(shop_guid=str(row['shop_guid'])).first()

        if df_find:
            df_find.shop_guid = row['shop_guid']
            df_find.shop_name = row['shop_name']
            df_find.shop_department = row['shop_department']
            df_find.manager = row['manager']            


        else:
            new_df = JSretail_shops(
            shop_guid = row['shop_guid'],
            shop_name = row['shop_name'],
            shop_department = row['shop_department'],
            manager = row['manager']
            
        )
            session.add(new_df)
    session.commit()

# SKU
def upsert_sku(utAccess, dbAccess):

    access = utAccess()
    login = access.retail_login
    password = access.retail_pass
    host = access.host
    # Part to edit
    url_sku = f'https://{login}:{password}@{host}/hs/discharge/sku/'
    sku = rq.get(url=url_sku, verify=False)

    if sku.status_code == 200:
        sku = pd.DataFrame.from_records(sku.json())
    else:
        sys.exit('Not 200')


    sku['macroclass'] = sku['sku_name'].apply(lambda x: classify_macro(x))
    sku['distr_class'] = sku['sku_name'].apply(lambda x: classify_sku(x))
    sku['sku_name'] = sku['sku_name'].apply(lambda x: replace_invalid_utf8(x))

    # End part
    hook = PostgresHook(postgres_conn_id='js_db', encoding='utf-8')
    engine = hook.get_sqlalchemy_engine()
    Session = sessionmaker(bind=engine)
    session = Session()


    for _, row in sku.iterrows():
        df_find = session.query(JSretail_sku).filter_by(sku_guid=str(row['sku_guid'])).first()

        if df_find:
            df_find.sku_guid = row['sku_guid']
            df_find.sku_name = row['sku_name']
            df_find.macroclass = row['macroclass']
            df_find.distr_class = row['distr_class']
           


        else:
            new_df = JSretail_sku(
            sku_guid = row['sku_guid'],
            sku_name = row['sku_name'],
            macroclass = row['macroclass'],
            distr_class = row['distr_class']
            
        )
            session.add(new_df)
    session.commit()


