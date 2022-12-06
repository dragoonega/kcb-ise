import sqlalchemy as db
import pandas as pd
from sqlalchemy import *


def conndb2():
# membuat koneksi dengan database menggunakan SQLAlchemy
    global engine,conn2,metadata
    engine = db.create_engine("mssql+pymssql://FPKCB:fpkcb123@localhost:1433/fp_kcb")  
    conn2 = engine.connect()
    
def droptabledimension():
    conn2.execute('DROP TABLE IF EXISTS Dimension_City;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Participant;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Region;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Payment;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Source;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Team;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Time;'),
    conn2.execute('DROP TABLE IF EXISTS Dimension_Instance;'),

def droptablefact():
    conn2.execute('DROP TABLE IF EXISTS Fact_Participant;'),
    conn2.execute('DROP TABLE IF EXISTS Fact_Sales;'),
    conn2.execute('DROP TABLE IF EXISTS Fact_Source'),
    conn2.execute('DROP TABLE IF EXISTS Fact_Teams;'),

def createtabledimension():
    global dim_part,dim_city,dim_instance,dim_payment,dim_region,dim_time,dim_source,dim_team
    metadata = MetaData(bind=engine)
    dim_source = Table('Dimension_Source', metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('Source', Text,
                              ),
                       )
    dim_source.drop(checkfirst=True)
    dim_source.create()
    
    dim_part = Table('Dimension_Participant', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('full_name', String),
                Column('email', String),
                Column('category', String ),
            )
            
    dim_part.drop(checkfirst=True)
    dim_part.create()

    dim_city = Table('Dimension_City', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('name', String),
                Column('region', String ),
            )
            
    dim_city.drop(checkfirst=True)
    dim_city.create()

    dim_region = Table('Dimension_Region', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('region_id', Integer),
                Column('region_name', String )
            )
            
    dim_region.drop(checkfirst=True)
    dim_region.create()

    dim_instance = Table('Dimension_Instance', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('name', String, ),
                Column('class', String ),
                Column('Major', String ),
            )
            
    dim_instance.drop(checkfirst=True)
    dim_instance.create()

    dim_payment = Table('Dimension_Payment', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('price', Integer ),
                Column('status', String ),
                Column('method', String)
            )
            
    dim_payment.drop(checkfirst=True)
    dim_payment.create()

    dim_team = Table('Dimension_Team', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('name', String )
            )
            
    dim_team.drop(checkfirst=True)
    dim_team.create()

    dim_time = Table('Dimension_Time', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('date', Date ),
                Column('time', Time)
            )
            
    dim_time.drop(checkfirst=True)
    dim_time.create()
    
def createFactTable():
    metadata = MetaData(bind=engine)
    fact_participant = Table('Fact_Participant', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('participant_name', String),
            Column('participant_category', String),
            Column('city_name', String),
            Column('region_name', String),
            Column('instance_name', String),
            Column('team_name', String)
            )

    fact_participant.drop(checkfirst=True)
    fact_participant.create()
    
    fact_source = Table('Fact_Source', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('participant_name', String),
            Column('participant_category', String),
            Column('source_name', Text ),
            )

    fact_source.drop(checkfirst=True)
    fact_source.create()

    fact_teams = Table('Fact_Teams', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('team_category', String),
            Column('team_name', String),
            Column('city_name', String),
            Column('region_name', String),
            Column('instance_name', String)
            )

    fact_teams.drop(checkfirst=True)
    fact_teams.create() 

    fact_sales = Table('Fact_Sales', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('participant_name', String),
            Column('participant_category', String),
            Column('method', String),
            Column('price', Integer ),
            Column('sum_price', Integer, )
        )


    fact_sales.drop(checkfirst=True)
    fact_sales.create()
    
def loaddatasource():
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Participant ON;')
    # Load data from database source 
    source_participant = pd.read_csv('csv/Dim_Participant.csv')
    source_participant.to_sql('Dimension_Participant', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Participant Off;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Instance ON;')
    source_instance = pd.read_csv('csv/Dim_Instance.csv')
    source_instance.to_sql('Dimension_Instance', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Instance OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_City ON;')
    source_city = pd.read_csv('csv/Dim_City.csv')
    source_city.to_sql('Dimension_City', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_City OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Region ON;')
    source_region = pd.read_csv('csv/Dim_Region.csv')
    source_region.to_sql('Dimension_Region', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Region OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Team ON;')
    source_team = pd.read_csv('csv/Dim_Team.csv')
    source_team.to_sql('Dimension_Team', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Team OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Payment ON;')
    source_payment = pd.read_csv('csv/Dim_Payment.csv')
    source_payment.to_sql('Dimension_Payment', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Payment OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Time ON;')
    source_time = pd.read_csv('csv/Dim_Time.csv')
    source_time.to_sql('Dimension_Time', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Time OFF;')
    
    conn2.execute('SET IDENTITY_INSERT Dimension_Source ON;')
    source_source = pd.read_csv('csv/Dim_Source.csv')
    source_source.to_sql('Dimension_Source', conn2, if_exists='append', index=False)
    conn2.execute('SET IDENTITY_INSERT Dimension_Source OFF;')
    
conndb2()
droptablefact()
droptabledimension()
createtabledimension()
createFactTable()
loaddatasource()
