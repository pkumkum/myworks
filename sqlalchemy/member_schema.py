import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship,backref
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import Table

Base = declarative_base()
class Member(Base):
    __tablename__ = 'Member'
    Member_id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
 
class Member_Address(Base):
    __tablename__ = 'Member_Address'
    id = Column(Integer, primary_key=True)
    addr1 = Column(String(250))
    addr2 = Column(String(250))
    pin = Column(String(250), nullable=False)
    Member_id = Column(Integer, ForeignKey('Member.Member_id'))
    Member = relationship(Member)
 
engine = create_engine('mssql+pyodbc://sa:sa123@test')
Base.metadata.create_all(engine)#creates the table structure