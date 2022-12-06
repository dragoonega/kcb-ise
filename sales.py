#!/usr/bin/python

import pandas as pd
import math
from onetimehistory import conndb2,conn2

def sales():
    conndb2()
    # Extract
    conn2.execute('SET IDENTITY_INSERT Fact_Sales ON;')
    participant = pd.read_csv('csv/Dim_Participant.csv')
    payment = pd.read_csv('csv/Dim_Payment.csv')

    # Transform
    participant = participant.rename(columns={'full_name': 'participant_name', 'category': 'participant_category'})
    
    participant = participant[['id', 'participant_name', 'participant_category']]
    payment = payment[['id', 'method', 'price']]
    
    sales = pd.merge(participant, payment, on='id', how='inner')

    sales.insert(5, 'sum_price', sales['price'])
    for i, _ in sales.iterrows():
        if math.isnan(sales.loc[i,'price']):
            sales.loc[i,'price']=0
        if i == 0:
            sales.loc[i,'sum_price']=sales.loc[i,'sum_price']
        else:
            sales.loc[i,'sum_price']=sales.loc[i-1,'sum_price']+sales.loc[i,'price']

    # Load
    sales.to_sql('Fact_Sales', conn2, if_exists='append', index=False) 
    conn2.execute('SET IDENTITY_INSERT Fact_Sales OFF;')
    
sales()