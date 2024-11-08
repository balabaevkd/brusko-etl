
from models import Base, WB_orders, WB_sales
from access_ import mpAccess, dbAccess
import pandas as pd
import requests as rq
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import sqlalchemy as db
from sqlalchemy import Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, select
from sqlalchemy.orm import declarative_base, sessionmaker
import sys

def upsert_wb_orders():
    
    seller_list = [mpAccess.irm_token, mpAccess.kerwb_token, mpAccess.oawb_token, mpAccess.aliwb_token]


    for i in seller_list:
        print('Начало апсерта заказов')
        wb_token = i
        mpdb = dbAccess.mpdb

        # Методы для API
        orders_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/orders'

        border_date = datetime.today() - timedelta(days=90)
        # Получение данных по API и создание таблиц
        print('Апи запрос заказов')
        orders = rq.get(orders_url, headers={'Authorization' : wb_token}, params={'dateFrom' : str(border_date)})
        print(f'Код ответа {orders.status_code}')

        if orders.status_code == 200:
            orders = pd.DataFrame(orders.json())
        else:
            sys.exit('Not 200')
        
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

        


        print('Итерация')
        for _, row in orders.iterrows():
            df_find = session.query(WB_orders).filter_by(srid=str(row['srid'])).first()

            if df_find:
                df_find.date=row['date']
                df_find.lastChangeDate=row['lastChangeDate']
                df_find.warehouseName=row['warehouseName']
                df_find.warehouseType=row['warehouseType']
                df_find.countryName=row['countryName']
                df_find.oblastOkrugName=row['oblastOkrugName']
                df_find.regionName=row['regionName']
                df_find.supplierArticle=row['supplierArticle']
                df_find.nmId=row['nmId']
                df_find.barcode=row['barcode']
                df_find.category=row['category']
                df_find.subject=row['subject']
                df_find.brand=row['brand']
                df_find.techSize=row['techSize']
                df_find.incomeID=row['incomeID']
                df_find.isSupply=row['isSupply']
                df_find.isRealization=row['isRealization']
                df_find.totalPrice=row['totalPrice']
                df_find.discountPercent=row['discountPercent']
                df_find.spp=row['spp']
                df_find.finishedPrice=row['finishedPrice']
                df_find.priceWithDisc=row['priceWithDisc']
                df_find.isCancel=row['isCancel']
                df_find.cancelDate=row['cancelDate']
                df_find.orderType=row['orderType']
                df_find.sticker=row['sticker']
                df_find.gNumber=row['gNumber']
                df_find.srid=row['srid']


            else:
                new_df = WB_orders(
                date=row['date'],
                lastChangeDate=row['lastChangeDate'],
                warehouseName=row['warehouseName'],
                warehouseType=row['warehouseType'],
                countryName=row['countryName'],
                oblastOkrugName=row['oblastOkrugName'],
                regionName=row['regionName'],
                supplierArticle=row['supplierArticle'],
                nmId=row['nmId'],
                barcode=row['barcode'],
                category=row['category'],
                subject=row['subject'],
                brand=row['brand'],
                techSize=row['techSize'],
                incomeID=row['incomeID'],
                isSupply=row['isSupply'],
                isRealization=row['isRealization'],
                totalPrice=row['totalPrice'],
                discountPercent=row['discountPercent'],
                spp=row['spp'],
                finishedPrice=row['finishedPrice'],
                priceWithDisc=row['priceWithDisc'],
                isCancel=row['isCancel'],
                cancelDate=row['cancelDate'],
                orderType=row['orderType'],
                sticker=row['sticker'],
                gNumber=row['gNumber'],
                srid=row['srid']
            )
                session.add(new_df)
        session.commit()

def upsert_wb_sales():

    seller_list = [mpAccess.irm_token, mpAccess.kerwb_token, mpAccess.oawb_token, mpAccess.aliwb_token]

    for i in seller_list:
        wb_token = i
        mpdb = dbAccess.mpdb

        sales_url = 'https://statistics-api.wildberries.ru/api/v1/supplier/sales'
        border_date = datetime.today() - timedelta(days=90)
        sales = rq.get(sales_url, headers={'Authorization' : wb_token}, params={'dateFrom' : str(border_date)})
        
        if sales.status_code == 200:
            sales = pd.DataFrame(sales.json())
        else:
            sys.exit('Not 200')
        
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

        for _, row in sales.iterrows():
            df_find = session.query(WB_sales).filter_by(srid=str(row['srid'])).first()

            if df_find:
                df_find.date = row['date']
                df_find.lastChangeDate = row['lastChangeDate']
                df_find.warehouseName = row['warehouseName']
                df_find.warehouseType = row['warehouseType']
                df_find.countryName = row['countryName']
                df_find.oblastOkrugName = row['oblastOkrugName']
                df_find.regionName = row['regionName']
                df_find.supplierArticle = row['supplierArticle']
                df_find.nmId = row['nmId']
                df_find.barcode = row['barcode']
                df_find.category = row['category']
                df_find.subject = row['subject']
                df_find.brand = row['brand']
                df_find.techSize = row['techSize']
                df_find.incomeID = row['incomeID']
                df_find.isSupply = row['isSupply']
                df_find.isRealization = row['isRealization']
                df_find.totalPrice = row['totalPrice']
                df_find.discountPercent = row['discountPercent']
                df_find.spp = row['spp']
                df_find.paymentSaleAmount = row['paymentSaleAmount']
                df_find.forPay = row['forPay']
                df_find.finishedPrice = row['finishedPrice']
                df_find.priceWithDisc = row['priceWithDisc']
                df_find.saleID = row['saleID']
                df_find.orderType = row['orderType']
                df_find.sticker = row['sticker']
                df_find.gNumber = row['gNumber']
                df_find.srid = row['srid']


            else:
                new_df = WB_sales(
                    date = row["date"],
                    lastChangeDate = row["lastChangeDate"],
                    warehouseName = row["warehouseName"],
                    warehouseType = row["warehouseType"],
                    countryName = row["countryName"],
                    oblastOkrugName = row["oblastOkrugName"],
                    regionName = row["regionName"],
                    supplierArticle = row["supplierArticle"],
                    nmId = row["nmId"],
                    barcode = row["barcode"],
                    category = row["category"],
                    subject = row["subject"],
                    brand = row["brand"],
                    techSize = row["techSize"],
                    incomeID = row["incomeID"],
                    isSupply = row["isSupply"],
                    isRealization = row["isRealization"],
                    totalPrice = row["totalPrice"],
                    discountPercent = row["discountPercent"],
                    spp = row["spp"],
                    paymentSaleAmount = row["paymentSaleAmount"],
                    forPay = row["forPay"],
                    finishedPrice = row["finishedPrice"],
                    priceWithDisc = row["priceWithDisc"],
                    saleID = row["saleID"],
                    orderType = row["orderType"],
                    sticker = row["sticker"],
                    gNumber = row["gNumber"],
                    srid = row["srid"]
            )
                session.add(new_df)
        session.commit()


