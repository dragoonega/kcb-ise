import pandas as pd
import os
import math


# delete file if exist
if os.path.exists('csv/Fact_Sales.csv'):
    os.remove('csv/Fact_Sales.csv')


# Read data
participants=pd.read_csv('csv/Dim_Participant.csv')
payments=pd.read_csv('csv/Dim_Payment.csv')
# Merge data
sales=pd.merge(participants,payments,on=['id'],how='inner')


# create new column named 'sum_price'
sales.insert(5,'sum_price',sales['price'])
for i,row in sales.iterrows():
    if math.isnan(sales.loc[i,'price']):
        sales.loc[i,'price']=0

    if i==0:
        sales.loc[i,'sum_price']=sales.loc[i,'sum_price']
    else:
        sales.loc[i,'sum_price']=sales.loc[i-1,'sum_price']+sales.loc[i,'price']

# create new file named fact_sales.csv
sales.to_csv('csv/Fact_Sales.csv',index=False)
