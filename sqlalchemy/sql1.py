from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String,DateTime
from sqlalchemy.orm import sessionmaker
Base = declarative_base()

class sys_user(Base):
    __tablename__='sys_user'
    user_idn=Column(Integer,primary_key=True)
    name=Column(String)    

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
session.commit()

for name, in session.query(sys_user.name).\
    filter(sys_user.name==None):
	    print name



   