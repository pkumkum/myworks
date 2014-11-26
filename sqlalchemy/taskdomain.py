from sqlalchemy import create_engine, String, Date, Integer, ForeignKey, Column, Text, Sequence, select, func,DateTime,CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, deferred, synonym, contains_eager
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import except_
import datetime
from datetime import timedelta

#engine = create_engine('postgres://training:help123@/training',echo=True)
engine = create_engine('sqlite:///data.db', echo=False)

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class User(Base):
    """ User"""

    __tablename__ = "user"

    id = Column(Integer, Sequence('user_id'), primary_key=True)
    name = Column(String, nullable=False)     
    role = Column(String, nullable=False) 
    
    def __repr__(self):
        return '<User ID:%d Name: %s,Role %s>' % (self.id,self.name, self.role)

class Team(Base):
    """ Team class"""

    __tablename__ = "team"

    id = Column(Integer, Sequence('team_id'), primary_key=True)
    name = Column(String, nullable=False)     
    managerid  = Column(Integer, ForeignKey('user.id'), nullable=False) 
    
    def __repr__(self):
        return '<Team  ID: %d ,Name: %s,Managerid %d>' % (self.id,self.name, self.managerid)

class UserTeam(Base):
    """ UserTeam class"""

    __tablename__ = "userteam"

    id = Column(Integer, Sequence('userteam_id'), primary_key=True)   
    userid  = Column(Integer, ForeignKey('user.id'), nullable=False) 
    teamid  = Column(Integer, ForeignKey('team.id'), nullable=False)   

    def __repr__(self):
        return '<ID: %d ,UserID: %d ,TeamID %d>' % (self.id,self.userid ,self.teamid)


class Task(Base):
    """ Task class"""

    __tablename__ = "task"

    id = Column(Integer, Sequence('task_id'), primary_key=True)
    name = Column(String, nullable=False) 
    description = Column(String, nullable=False) 
    priority  = Column(Integer, CheckConstraint('priority>=1','priority<=5'),nullable=False) 
    starttime = Column(DateTime, default=func.now()) 
    endtime = Column(Integer, nullable=False, default= datetime.datetime.now() + timedelta(days=7))     

    def __repr__(self):
        return '<Task Name: %s,Description:%s, Priority :%s ,starttime :%s ,endtime : %s>' % (self.name, self.description,self.priority,self.starttime,self.endtime)

class TeamTask(Base):
    """ Task class"""

    __tablename__ = "teamtask"

    id = Column(Integer, Sequence('usertask_id'), primary_key=True)
    teamid  = Column(Integer, ForeignKey('user.id'), nullable=False) 
    taskid = Column(Integer, ForeignKey('task.id'), nullable=False)         

    def __repr__(self):
        return '<TeamID: %d,Task ID %d>' % (self.teamid, self.taskid)


class UserTask(Base):
    """ Task class"""

    __tablename__ = "usertask"

    id = Column(Integer, Sequence('usertask_id'), primary_key=True)
    userid  = Column(Integer, ForeignKey('user.id'), nullable=False) 
    taskid  = Column(Integer, ForeignKey('task.id'), nullable=False) 
    completion = Column(Float, nullable=False)     

    def __repr__(self):
        return '<Task ID:%d  userid: %d,taskid : %d>' % (self.id,self.userid, self.taskid)

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Jack = User(name='Jack',role='user')
Jill = User(name='Jill',role='user')
Johnny = User(name='Johnny',role='user')
Jamie = User(name='Jamie',role='user')
Joseph = User(name='Joseph',role='manager')
Jacob = User(name='Jacob',role='manager')

session.add_all([Jack,Jill,Jamie,Johnny,Joseph,Jacob])
print session.query(User).all()
session.commit()

teamjoseph = Team(name='Team Joseph',managerid =Joseph.id)
teamjacob = Team(name = 'Team Jacob' ,managerid = Jacob.id )
session.commit()

session.add_all([teamjacob,teamjoseph])
print session.query(Team).all()

jackuser = UserTeam(userid = Jack.id,teamid = teamjacob.id)
jilluser = UserTeam(userid = Jill.id,teamid = teamjacob.id)
johnnyuser = UserTeam(userid = Johnny.id,teamid = teamjoseph.id)
jamieuser = UserTeam(userid = Jamie.id,teamid = teamjoseph.id)
josephuser = UserTeam(userid = Joseph.id,teamid = teamjoseph.id)
jacobuser = UserTeam(userid = Jacob.id,teamid = teamjacob.id)

session.add_all([jackuser,johnnyuser,jamieuser,josephuser,jacobuser])
session.commit()

print session.query(UserTeam).all()

task1 = Task (name= 'Learn SQL-Alchemy ORM' , description='Learn SQL-Alchemy ORM',priority = 5,endtime = datetime.datetime.now() + timedelta(days=5))
task2 = Task (name= 'Plan Migration to SQL Alchemy ' , description='Plan Migration to SQL Alchemy ',priority = 3,endtime = datetime.datetime.now() + timedelta(days=90))
task3 = Task (name= 'Learn Python ' , description='Learn Python ',priority = 4,endtime = datetime.datetime.now() + timedelta(days=30))

session.add_all([task1,task2,task3])
session.commit()
print session.query(Task).all()

taskjoseph = TeamTask(teamid= teamjoseph.id,taskid = task1.id)
taskjoseph1 = TeamTask(teamid= teamjoseph.id,taskid = task3.id)
taskjacob = TeamTask(teamid= teamjacob.id,taskid = task2.id)
taskjacob1 = TeamTask(teamid= teamjacob.id,taskid = task3.id)

session.add_all([taskjoseph,taskjoseph1,taskjacob,taskjacob1])
session.commit()
print session.query(TeamTask).all()


jacktask = UserTask(userid = Jack.id,taskid = task1.id,completion= 50.5)
jacktask1 = UserTask(userid = Jack.id,taskid = task2.id,completion= 45.5)
jacktask2 = UserTask(userid = Jack.id,taskid = task3.id,completion= 95)
jilltask = UserTask(userid = Jill.id,taskid = task1.id,completion= 35.5)
jilltask1 = UserTask(userid = Jill.id,taskid = task2.id,completion= 35.5)
jilltask2 = UserTask(userid = Jill.id,taskid = task3.id,completion= 35.5)
johnnytask = UserTask(userid = Johnny.id,taskid = task1.id,completion= 25.5)
johnnytask1 = UserTask(userid = Johnny.id,taskid = task3.id,completion= 85.5)
jamietask = UserTask(userid = Jamie.id,taskid = task1.id,completion= 80)
jamietask1 = UserTask(userid = Jamie.id,taskid = task3.id,completion= 18)
josephtask = UserTask(userid = Joseph.id,taskid = task1.id,completion= 50)
josephtask1 = UserTask(userid = Joseph.id,taskid = task3.id,completion= 60)
jacobtask = UserTask(userid = Jacob.id,taskid = task1.id,completion= 100) 
jacobtask1 = UserTask(userid = Jacob.id,taskid = task3.id,completion= 100) 


session.add_all([jacktask,jacktask1,jacktask2,jilltask,jilltask1,jilltask2,johnnytask,johnnytask1,jamietask,jamietask1,josephtask,josephtask1,jacobtask,jacobtask1])
session.commit()
print session.query(UserTask).all()
print '-----------------------------------------------------------------'

#Given a manager name, list all pending tasks assigned to his employees
rs1 =  session.query(distinct(Task.name)).join(UserTask).join(User).join(UserTeam).join(Team).filter(and_(UserTask.completion < 100,User.name == 'Joseph')).all()
print rs1

#Given a task name, print the count of users actively working on the task.

rs2 = session.query(func.count(UserTask.userid)).join(Task).filter(Task.name=='Learn SQL-Alchemy ORM').group_by(Task.id).all()
print rs2