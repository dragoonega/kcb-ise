#!/usr/bin/python

import pandas as pd
from onetimehistory import conndb2,conn2

# Participant Fact Table
def participant():
    conndb2()
    # Extract
    participant = pd.read_csv('csv/Dim_Participant.csv')
    city = pd.read_csv('csv/Dim_City.csv')
    region = pd.read_csv('csv/Dim_Region.csv')
    instance = pd.read_csv('csv/Dim_Instance.csv')
    team = pd.read_csv('csv/Dim_Team.csv')

    # Transform
    participant = participant.rename(columns={'full_name': 'participant_name', 'category': 'participant_category'})    
    city = city.rename(columns={'name': 'city_name', 'region': 'region_id'})
    region = region.rename(columns={'name': 'region_name'})
    instance = instance.rename(columns={'name': 'instance_name'})
    team = team.rename(columns={'name': 'team_name'})
    
    participant = participant[['id', 'participant_name', 'participant_category']]
    instance = instance[['id', 'instance_name']]

    city = pd.merge(city, region, how='inner', on='region_id')
    city = city[['id', 'city_name', 'region_name']]

    fact = pd.merge(participant, city, how='inner', on='id')
    fact = pd.merge(fact, instance, how='inner', on='id')
    fact = pd.merge(fact, team, how='inner', on='id')

    # Load
    fact.to_sql('Fact_Participant', conn2, if_exists='append', index=False)
    
participant()