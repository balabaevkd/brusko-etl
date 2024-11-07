import pandas as pd
import datetime
import requests as rq
import pytz
import time
import numpy as np

from models import Base, OZON_orders

from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
import sys
from access_ import mpAccess


def transform_date(date):
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    utc_date = date.astimezone(datetime.timezone.utc)
    return utc_date.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def rename_cols():
    return {'Номер заказа' : 'order_id', 
            'Номер отправления' : 'posting_id', 'Принят в обработку' : 'date_ordered', 'Дата отгрузки' : 'date_processed', 'Статус' : 'status', 'Дата доставки' : 'date_delivered', 'Фактическая дата передачи в доставку' : 'date_logistics_fact', 'Сумма отправления' : 'posting_sum', 'Код валюты отправления' : 'posting_currency_code', 'Наименование товара' : 'product_name', 'OZON id' : 'ozon_id', 'Артикул' : 'article', 'Итоговая стоимость товара' : 'total_price', 'Код валюты товара' : 'product_currency_code', 'Стоимость товара для покупателя' : 'customer_price', 'Код валюты покупателя' : 'customer_currency_code', 'Количество' : 'amount', 'Стоимость доставки' : 'logistics_cost', 'Связанные отправления' : 'related_postings', 'Выкуп товара' : 'product_sold', 'Цена товара до скидок' : 'price_nodisc', 'Скидка %' : 'discount_percent', 'Скидка руб' : 'discount_rub', 'Акции' : 'actions', 'Объемный вес товаров, кг' : 'volume'}


def upsert_ozon_fbo_orders():

    seller_list = [mpAccess.geroy, mpAccess.healthy, mpAccess.sexologic, mpAccess.probaits]

    for creds in seller_list:
    
        high_border = datetime.datetime.now().astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        low_border = (datetime.datetime.now() - datetime.timedelta(days=90)).astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        create_postings_url = 'https://api-seller.ozon.ru/v1/report/postings/create'

        code = rq.post(create_postings_url, headers=creds, 
                    json={
                        'filter' : {
                            'delivery_schema' : ['fbo'],
                            'processed_at_from' : low_border,
                            'processed_at_to' : high_border
                        }  
                    })
        if code.status_code == 200:
            print('Код получен')
            code = code.json()['result']['code']
        else:
            print(f'Запрос вернул {code.status_code}. Роняю таск')
            exit

        info_url = 'https://api-seller.ozon.ru/v1/report/info'
        print('Жду 3 секунды для формирования отчета')
        time.sleep(3)
        try:
            report = rq.post(url=info_url, headers=creds, json={'code' : code})
            report_file = report.json()['result']['file']
            print('Отчет получен')
        except:
            print('Ошибка, файл еще не готов. Сплю 5 секунд')
            time.sleep(5)
            report = rq.post(url=info_url, headers=creds, json={'code' : code})


        orders = pd.read_csv(report_file, sep=';')
        orders = orders.rename(columns=rename_cols())

        orders = orders.replace(np.nan, None)

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

        for _, row in orders.iterrows():
            df_find = session.query(OZON_orders).filter_by(posting_id=str(row['posting_id'])).first()

            if df_find:
                df_find.order_id=row['order_id']
                df_find.posting_id=row['posting_id']
                df_find.date_ordered=row['date_ordered']
                df_find.date_processed=row['date_processed']
                df_find.status=row['status']
                df_find.date_delivered=row['date_delivered']
                df_find.date_logistics_fact=row['date_logistics_fact']
                df_find.posting_sum=row['posting_sum']
                df_find.posting_currency_code=row['posting_currency_code']
                df_find.product_name=row['product_name']
                df_find.ozon_id=row['ozon_id']
                df_find.article=row['article']
                df_find.total_price=row['total_price']
                df_find.product_currency_code=row['product_currency_code']
                df_find.customer_price=row['customer_price']
                df_find.customer_currency_code=row['customer_currency_code']
                df_find.amount=row['amount']
                df_find.logistics_cost=row['logistics_cost']
                df_find.related_postings=row['related_postings']
                df_find.product_sold=row['product_sold']
                df_find.price_nodisc=row['price_nodisc']
                df_find.discount_percent=row['discount_percent']
                df_find.discount_rub=row['discount_rub']
                df_find.actions=row['actions']
                df_find.volume=row['volume']
            else:
                new_df = OZON_orders(
                    order_id=row['order_id'],
                posting_id=row['posting_id'],
                date_ordered=row['date_ordered'],
                date_processed=row['date_processed'],
                status=row['status'],
                date_delivered=row['date_delivered'],
                date_logistics_fact=row['date_logistics_fact'],
                posting_sum=row['posting_sum'],
                posting_currency_code=row['posting_currency_code'],
                product_name=row['product_name'],
                ozon_id=row['ozon_id'],
                article=row['article'],
                total_price=row['total_price'],
                product_currency_code=row['product_currency_code'],
                customer_price=row['customer_price'],
                customer_currency_code=row['customer_currency_code'],
                amount=row['amount'],
                logistics_cost=row['logistics_cost'],
                related_postings=row['related_postings'],
                product_sold=row['product_sold'],
                price_nodisc=row['price_nodisc'],
                discount_percent=row['discount_percent'],
                discount_rub=row['discount_rub'],
                actions=row['actions'],
                volume=row['volume'] 
                )
                session.add(new_df)
            session.commit()

def upsert_ozon_fbs_orders():

    seller_list = [mpAccess.geroy, mpAccess.healthy, mpAccess.sexologic, mpAccess.probaits]

    for creds in seller_list:
    
        high_border = datetime.datetime.now().astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        low_border = (datetime.datetime.now() - datetime.timedelta(days=90)).astimezone(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        create_postings_url = 'https://api-seller.ozon.ru/v1/report/postings/create'

        code = rq.post(create_postings_url, headers=creds, 
                    json={
                        'filter' : {
                            'delivery_schema' : ['fbs'],
                            'processed_at_from' : low_border,
                            'processed_at_to' : high_border
                        }  
                    })
        if code.status_code == 200:
            print('Код получен')
            code = code.json()['result']['code']
        else:
            print(f'Запрос вернул {code.status_code}. Роняю таск')
            exit

        info_url = 'https://api-seller.ozon.ru/v1/report/info'
        print('Жду 3 секунды для формирования отчета')
        time.sleep(3)
        try:
            report = rq.post(url=info_url, headers=creds, json={'code' : code})
            report_file = report.json()['result']['file']
            print('Отчет получен')
        except:
            print('Ошибка, файл еще не готов. Сплю 5 секунд')
            time.sleep(5)
            report = rq.post(url=info_url, headers=creds, json={'code' : code})


        orders = pd.read_csv(report_file, sep=';')
        orders = orders.rename(columns=rename_cols())

        orders = orders.replace(np.nan, None)

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

        for _, row in orders.iterrows():
            df_find = session.query(OZON_orders).filter_by(posting_id=str(row['posting_id'])).first()

            if df_find:
                df_find.order_id=row['order_id']
                df_find.posting_id=row['posting_id']
                df_find.date_ordered=row['date_ordered']
                df_find.date_processed=row['date_processed']
                df_find.status=row['status']
                df_find.date_delivered=row['date_delivered']
                df_find.date_logistics_fact=row['date_logistics_fact']
                df_find.posting_sum=row['posting_sum']
                df_find.posting_currency_code=row['posting_currency_code']
                df_find.product_name=row['product_name']
                df_find.ozon_id=row['ozon_id']
                df_find.article=row['article']
                df_find.total_price=row['total_price']
                df_find.product_currency_code=row['product_currency_code']
                df_find.customer_price=row['customer_price']
                df_find.customer_currency_code=row['customer_currency_code']
                df_find.amount=row['amount']
                df_find.logistics_cost=row['logistics_cost']
                df_find.related_postings=row['related_postings']
                df_find.product_sold=row['product_sold']
                df_find.price_nodisc=row['price_nodisc']
                df_find.discount_percent=row['discount_percent']
                df_find.discount_rub=row['discount_rub']
                df_find.actions=row['actions']
                #df_find.volume=row['volume']
            else:
                new_df = OZON_orders(
                    order_id=row['order_id'],
                    posting_id=row['posting_id'],
                    date_ordered=row['date_ordered'],
                    date_processed=row['date_processed'],
                    status=row['status'],
                    date_delivered=row['date_delivered'],
                    date_logistics_fact=row['date_logistics_fact'],
                    posting_sum=row['posting_sum'],
                    posting_currency_code=row['posting_currency_code'],
                    product_name=row['product_name'],
                    ozon_id=row['ozon_id'],
                    article=row['article'],
                    total_price=row['total_price'],
                    product_currency_code=row['product_currency_code'],
                    customer_price=row['customer_price'],
                    customer_currency_code=row['customer_currency_code'],
                    amount=row['amount'],
                    logistics_cost=row['logistics_cost'],
                    related_postings=row['related_postings'],
                    product_sold=row['product_sold'],
                    price_nodisc=row['price_nodisc'],
                    discount_percent=row['discount_percent'],
                    discount_rub=row['discount_rub'],
                    actions=row['actions']
                    #volume=row['volume'] 
                )
                session.add(new_df)
            session.commit()





