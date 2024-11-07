from sqlalchemy import MetaData, Table, String, Integer, Column, Text, DateTime, Boolean, ForeignKey, insert, Float
from sqlalchemy.orm import declarative_base
import sqlalchemy as db


metadata = MetaData()
Base = declarative_base(metadata=metadata)

class WB_orders(Base):
    __tablename__ = 'wb_orders'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    lastChangeDate = Column(DateTime)
    warehouseName = Column(String)
    warehouseType = Column(String)
    countryName = Column(String)
    oblastOkrugName = Column(String)
    regionName = Column(String)
    supplierArticle = Column(String)
    nmId = Column(Integer)
    barcode = Column(String)
    category = Column(String)
    subject = Column(String)
    brand = Column(String)
    techSize = Column(String)
    incomeID = Column(Integer)
    isSupply = Column(Boolean)
    isRealization = Column(Boolean)
    totalPrice = Column(Integer)
    discountPercent = Column(Integer)
    spp = Column(Integer)
    finishedPrice = Column(Integer)
    priceWithDisc = Column(Integer)
    isCancel = Column(Boolean)
    cancelDate = Column(DateTime)
    orderType = Column(String)
    sticker = Column(String)
    gNumber = Column(String)
    srid = Column(String)

class WB_sales(Base):
    __tablename__ = 'wb_sales'

    date = Column(DateTime)
    lastChangeDate = Column(DateTime)
    warehouseName = Column(String)
    warehouseType = Column(String)
    countryName = Column(String)
    oblastOkrugName = Column(String)
    regionName = Column(String)
    supplierArticle = Column(String)
    nmId = Column(Integer)
    barcode = Column(String)
    category = Column(String)
    subject = Column(String)
    brand = Column(String)
    techSize = Column(String)
    incomeID = Column(Integer)
    isSupply = Column(Boolean)
    isRealization = Column(Boolean)
    totalPrice = Column(Integer)
    discountPercent = Column(Integer)
    spp = Column(Integer)
    paymentSaleAmount = Column(Integer)
    forPay = Column(Float)
    finishedPrice = Column(Integer)
    priceWithDisc = Column(Integer)
    saleID = Column(String)
    orderType = Column(String)
    sticker = Column(String)
    gNumber = Column(String)
    srid = Column(String, primary_key=True)

class OZON_orders(Base):
    __tablename__ = 'ozon_orders'

    order_id = Column(String)
    posting_id = Column(String, primary_key=True)
    date_ordered = Column(DateTime)
    date_processed = Column(DateTime)
    status = Column(String)
    date_delivered = Column(DateTime)
    date_logistics_fact = Column(DateTime)
    posting_sum = Column(Float)
    posting_currency_code = Column(String)
    product_name = Column(String)
    ozon_id = Column(String)
    article = Column(String)
    total_price = Column(Float)
    product_currency_code = Column(String)
    customer_price = Column(Float)
    customer_currency_code = Column(String)
    amount = Column(Integer)
    logistics_cost = Column(Float)
    related_postings = Column(String)
    product_sold = Column(String)
    price_nodisc = Column(Float)
    discount_percent = Column(String)
    discount_rub = Column(Float)
    actions = Column(String)
    volume = Column(Float)


class OZON_conversion(Base):
    __tablename__ = 'ozon_conversion'

    ozon_id = Column(Integer, primary_key=True)
    name = Column(String)
    timestamp = Column(DateTime, primary_key=True)
    hits_view_search = Column(Integer)
    hits_view_pdp = Column(Integer)
    hits_tocart_search = Column(Integer)
    hits_tocart_pdp = Column(Integer)
    ordered_units = Column(Integer)
    position_category = Column(Float)




    
    
