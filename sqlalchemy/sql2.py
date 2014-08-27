from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String,DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship,backref

Base = declarative_base()

class sys_user(Base):
    __tablename__='sys_user'
    user_idn=Column(Integer,primary_key=True)
    name=Column(String)    

class Address(Base):
    __tablename__ = 'Address'
    id = Column(Integer ,primary_key = True)
    email = Column(String,nullable = False)
    user_idn = Column(Integer,ForeignKey('sys_user.user_idn'))
    user = relationship("sys_user",backref = backref('addresses',order_by =id))
class Phone(Base):
    __tablename__ = 'Phone'
    id = Column(Integer ,primary_key = True)
    phone = Column(String,nullable = False)
    user_idn = Column(Integer,ForeignKey('sys_user.user_idn'))
    user = relationship("sys_user",backref = backref('phonenos',order_by =id))

engine=create_engine('sqlite:///:memory:',echo=True)
Base.metadata.create_all(engine)

mbr_list = [(1,'tuttu' ),(2,'mittu' ),(3,'')]
DBSession = sessionmaker(bind = engine) 
session = DBSession()

for mbr in mbr_list:
    user = sys_user()
    user.user_idn = mbr[0]
    user.name = mbr[1]    
    session.add(user)


for name, in session.query(sys_user.name).\
    filter(sys_user.name==None):
    print name

user = sys_user ()
user.name = 'hari'

for email in ['a@b.com','c@d.com']:
    addr = Address()
    addr.email = email
    user.addresses.append(addr)

for phone in ['234234234','2342342']:
    ph = Phone()
    ph.phone = phone
    user.phonenos.append(ph)

session.add(user)
session.commit()
