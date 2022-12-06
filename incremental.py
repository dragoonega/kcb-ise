import sqlalchemy as db
import pandas as pd
from sqlalchemy import *

def conndb():
# membuat koneksi dengan database Asal menggunakan SQLAlchemy
    global engine,conn
    engine = db.create_engine("mssql+pymssql://KCBISE:dbise2022@localhost:1433/ISE")  
    conn = engine.connect()
    
def conndb2():
# membuat koneksi dengan database Tujuan menggunakan SQLAlchemy
    global engine,conn2,metadata
    engine = db.create_engine("mssql+pymssql://FPKCB:fpkcb123@localhost:1433/fp_kcb")  
    conn2 = engine.connect()
    
def readtargetdata():
    parti_dest = pd.read_sql_table('Fact_Participant', dest.conn2)
    parti_dest

def getdataparti():
    # Extracting data from database
    dim_participant = pd.read_sql_table('Dimension_Participant', conn)
    df = pd.Dataframe(dim_participant)
    df
    source_parti = getdataparti()
    source_parti
    
def changedata():
    #Get all Changes
    changes = source_parti[~source_parti.apply(tuple,
    1).isin(parti_dest.apply(tuple, 1))]
    changes
    #Get modified rows
    modified = changes[changes.Code.isin(parti_dest.Code)]
    modified
    ## Get new rows
    inserts = changes[~changes.Code.isin(parti_dest.Code)]
    inserts

def updatedata(df, table, key name):

    # ambil data participant
    participant = meta.tables['Dimension_Participant']

    # update
    p = update(participant)
    p = p.values({"participant_name": "bambang"})
    p = p.where(participant.c.participant_name == "Jennifer Ardelia")
    engine.execute(p)

    # mengambil data 
    sql = text("SELECT * from participant")

    # mengambil semua data final
    result = engine.execute(sql).fetchall()

# update to database
updatedata(modified, 'Dimension_Participant', 'participant_name')


    
