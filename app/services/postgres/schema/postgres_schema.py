from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.schema import PrimaryKeyConstraint

# create the table in here
Base = declarative_base()

class Menu(Base):
    __tablename__ = "menu"
    product_id = Column(String, primary_key = True)
    product_name = Column(String)
    price = Column(Integer)
    
class Sales(Base):
    __tablename__ = "sales"
    customer_id = Column(String)
    order_date = Column(DateTime) 
    product_id = Column(String)
    sale_id = Column(Integer, autoincrement=True, primary_key=True, default=None)

    
class Members(Base): 
    __tablename__ = "members"
    customer_id = Column(String, primary_key=True)
    join_date = Column(DateTime)
    