import pandas as pd
import os
import math

# delete file if exist
if os.path.exists('csv/Fact_Teams.csv'):
    os.remove('csv/Fact_Teams.csv')

# Read data
team=pd.read_csv('csv/Dim_Team.csv')
instance=pd.read_csv('csv/Dim_Instance.csv')
city=pd.read_csv('csv/Dim_City.csv')
# Merge data
teams=pd.merge(team,instance,on=['id'],how='inner')
teams=pd.merge(teams,city,on=['id'],how='inner')


# create new file named fact_sales.csv
teams.to_csv('csv/Fact_Teams.csv',index=False)
