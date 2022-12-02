import sqlalchemy as db
import pandas as pd
from sqlalchemy import *

def conndb():
# membuat koneksi dengan database menggunakan SQLAlchemy
    global engine,conn
    engine = db.create_engine("mssql+pymssql://KCBISE:dbise2022@localhost:1433/ISE")  
    conn = engine.connect()

# ETL dimension table from database

def dim_participant():
    # Extracting data from database
    dim_participant = pd.read_sql_table('Dimension_Participant', conn)
 
    # Load To CSV 
    dim_participant.to_csv('csv/Dim_Participant.csv', index=False)
     
def dim_city():
    # Extracting data from database
    dim_city = pd.read_sql_table('Dimension_City', conn)
    
    # Load To CSV 
    dim_city.to_csv('csv/Dim_City.csv', index=False)
    
def dim_instance():
    # Extracting data from database
    dim_instance = pd.read_sql_table('Dimension_Instance', conn)
     
    # Load To CSV 
    dim_instance.to_csv('csv/Dim_Instance.csv', index=False)
    
def dim_team():
    # Extracting data from database
    dim_team = pd.read_sql_table('Dimension_Team', conn)
     
    # Load To CSV 
    dim_team.to_csv('csv/Dim_Team.csv', index=False)

def dim_payment():
    # Extracting data from database
    dim_payment = pd.read_sql_table('Dimension_Payment', conn)
    
    # Load To CSV 
    dim_payment.to_csv('csv/Dim_Payment.csv', index=False)

def dim_region():
    # Extracting data from database
    dim_region = pd.read_sql_table('Dimension_Region', conn)
    
    # Load To CSV 
    dim_region.to_csv('csv/Dim_Region.csv', index=False)

def dim_source():
    # Extracting data from database
    dim_source= pd.read_sql_table('Dimension_Source', conn)
    
    # Load To CSV 
    dim_source.to_csv('csv/Dim_Source.csv', index=False)

def dim_time():
    # Extracting data from database
    dim_time= pd.read_sql_table('Dimension_Time', conn)
    
    # Load To CSV 
    dim_time.to_csv('csv/Dim_Time.csv', index=False)
    
def etl_dimensiontable():
    conndb()
    dim_participant()
    dim_city()
    dim_instance()
    dim_region()
    dim_payment()
    dim_team()
    dim_source()
    dim_time()

etl_dimensiontable()