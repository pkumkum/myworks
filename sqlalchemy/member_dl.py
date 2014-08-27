import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from sqlalchemy import Table
from member_schema import *

class member_dl():
    ####
    def __init__(self, member_id, name ,addr_id, addr1, addr2, pin):
        self.member_id = member_id
        self.name = name    
        self.addr_id = addr_id
        self.addr1 = addr1
        self.addr2 = addr2
        self.pin = pin
        
    ####
    def insert_member(self):
        engine = create_engine('mssql+pyodbc://sa:sa123@test')
        DBSession = sessionmaker(bind = engine) 
        session = DBSession()
        mbr = Member(Member_id = self.member_id , name = self.name)
        session.add(mbr)
        session.commit()
    ####
    def insert_member_address(self):
        engine = create_engine('mssql+pyodbc://sa:sa123@test')
        DBSession = sessionmaker(bind = engine) 
        session = DBSession()
        mbr = Member_Address(id = self.addr_id , addr1 = self.addr1 , addr2 = self.addr2 , pin = self.pin , Member_id = self.member_id)
        session.add(mbr)
        session.commit()
    ####
    def get_all_members(self):
        engine = create_engine('mssql+pyodbc://sa:sa123@test')
        metadata = MetaData(engine)
        mbr_object = Table('Member',metadata,autoload = True)
        s = mbr_object.select()
        rs = s.execute()
        for row in rs:
            print row
    ####
    def get_all_member_addr(self):
        engine = create_engine('mssql+pyodbc://sa:sa123@test')
        metadata = MetaData(engine)
        mbr_object = Table('Member_Address',metadata,autoload = True)
        s = mbr_object.select()
        rs = s.execute()
        for row in rs:
            print row