from sqlalchemy import MetaData, Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, Float
from sqlalchemy.orm import declarative_base, sessionmaker
import sqlalchemy as db

metadata = MetaData()
Base = declarative_base(metadata=metadata)

class JSretail_checks(Base):
    __tablename__ = 'checks'
    
    check_guid = Column(String, primary_key=True)
    datetime = Column(DateTime)
    shop_guid = Column(String)
    client_guid = Column(String)

class JSretail_items(Base):

    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    check_guid = Column(String, ForeignKey('checks.check_guid'))
    sku_guid = Column(String)
    pcs = Column(Integer)
    price = Column(Float)

class JSretail_clients(Base):

    # 'client_guid', 'name', 'birth_date', 'reg_date'

    __tablename__ = 'clients'

    client_guid = Column(String, primary_key=True)
    name = Column(String)
    birth_date = Column(DateTime)
    reg_date = Column(DateTime)

class JSretail_shops(Base):

    # 'shop_guid', 'shop_name', 'shop_department', 'manager'

    __tablename__ = 'shops'

    shop_guid = Column(String, primary_key=True)
    shop_name = Column(String)
    shop_department = Column(String)
    manager = Column(String)

class JSretail_sku(Base):
    
    __tablename__ = 'sku'
    # Основные колонки
    sku_guid = Column(String, primary_key=True)
    sku_name = Column(String)

    # Заполняются скриптами
    macroclass = Column(String)
    distr_class = Column(String)

    # Будет заполняться в дальнейших обновлениях
    basic_price = Column(Float)

class JSretail_supply(Base):

    __tablename__ = 'supply'

    order_guid = Column(String, primary_key=True)
    date = Column(DateTime)
    sku_guid = Column(String)
    pcs = Column(Integer)
    price = Column(Float)

class JSretail_report(Base):
    
    __tablename__ = 'retail_report'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    sku_guid = Column(String)
    shop_guid = Column(String)
    pcs = Column(Integer)
    total = Column(Float)

engine = db.create_engine('postgresql://admin:Gidras7Bibikaz@192.168.188.73:5438/js_db')
Base.metadata.create_all(engine)




