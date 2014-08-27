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
    member_id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
 
class Episode(Base):
    __tablename__ = 'Episode'
    id = Column(Integer, primary_key=True)
    Member_id = Column(Integer, ForeignKey('Member.member_id'))
    cert = Column(String(250), nullable=False)
    Member = relationship(Member)
    member = relationship("Member",backref = backref('episodes',order_by =id))
 
class Identification(Base):
    __tablename__ = 'Identification'
    id = Column(Integer, primary_key=True)
    number = Column(String(250), nullable=False) 
    Member_id = Column(Integer, ForeignKey('Member.member_id'))
    member = relationship("Member",backref = backref('identifications',order_by =id))

engine=create_engine('sqlite:///:memory:',echo=True)
Base.metadata.create_all(engine)

mbr_list = [(1,'tuttu',['1','2','3'] ,['fghh','zxdfsdfqwq']),(2,'mittu',['5','6','7'],['xyz12','asdasd1213'] ),(3,'chittu',['10','11'],['hhh','jjjj'])]
DBSession = sessionmaker(bind = engine) 
session = DBSession()

for mb in mbr_list:
    mbr = Member()    
    mbr.name = mb[1]    
    for ep in mb[2]:
        enc = Episode()
        enc.cert = ep
        mbr.episodes.append(enc)
    for no in mb[3]:
        idn = Identification()
        idn.number = no
        mbr.identifications.append(idn)
    session.add(mbr)
session.commit()
mbr  = session.query(Member).filter(Member.name == 'mittu')
print mbr
mbr.name = 'xyz'
print '----------------------'
session.commit()