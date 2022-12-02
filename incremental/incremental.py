from sqlalchemy import create_engine
import sqlalchemy as db
import pandas as pd
import pyodbc
import os
from sqlalchemy import *
# melakukan incremental loading dengan metode Destination Change Comparison


# connect dengan source database
src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)

# destination database
def conndb():
# membuat koneksi dengan database menggunakan SQLAlchemy
    global engine,conn
    engine = db.create_engine("mssql+pymssql://KCBISE:dbise2022@localhost:1433/ISE")  
    conn = engine.connect()

source = pd.read_sql_query(""" SELECT top 10
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, src_conn)
source

# Save the data to destination as the intial load. On the first run we load all data.
tbl_name = "stg_IncrementalLoadTest"
source.to_sql(tbl_name, engine, if_exists='replace', index=False)

# Read Target data into a dataframe
target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)
target

# Let's select two additional rows from the source. We have two new records
source = pd.read_sql_query(""" SELECT top 12
CustomerKey,GeographyKey,CustomerAlternateKey,Title,FirstName,MiddleName,LastName,NameStyle,BirthDate,MaritalStatus
FROM dbo.DimCustomer; """, src_conn)
source

# Also update a record. I will update the middle name for customerkey: 11006
source.loc[source.MiddleName =='G', ['MiddleName']] = 'Gina'
source

target.apply(tuple,1)

source.apply(tuple,1).isin(target.apply(tuple,1))

# detech changes. Get rows that are not present in the target.
changes = source[~source.apply(tuple,1).isin(target.apply(tuple,1))]
changes

# Get modified rows
modified = changes[changes.CustomerKey.isin(target.CustomerKey)]
modified

# Get new records
inserts = changes[~changes.CustomerKey.isin(target.CustomerKey)]
inserts

def update_to_sql(df, table_name, key_name):
    a = []
    table = table_name
    primary_key = key_name
    temp_table = f"{table_name}_temporary_table"
    for col in df.columns:
        if col == primary_key:
            continue
        a.append(f'"{col}"=s."{col}"')
    df.to_sql(temp_table, engine, if_exists='replace', index=False)
    update_stmt_1 = f'UPDATE public."{table}" f '
    update_stmt_2 = "SET "
    update_stmt_3 = ", ".join(a)
    update_stmt_4 = f' FROM public."{table}" t '
    update_stmt_5 = f' INNER JOIN (SELECT * FROM public."{temp_table}") AS s ON s."{primary_key}"=t."{primary_key}" '
    update_stmt_6 = f' Where f."{primary_key}"=s."{primary_key}" '
    update_stmt_7 = update_stmt_1 + update_stmt_2 + update_stmt_3 + update_stmt_4 + update_stmt_5 +  update_stmt_6 +";"
    print(update_stmt_7)
    with engine.begin() as cnx:
        cnx.execute(update_stmt_7)

# Call update function
update_to_sql(modified, "stg_IncrementalLoadTest", "CustomerKey")

target = pd.read_sql('Select * from public."stg_IncrementalLoadTest"', engine)
target