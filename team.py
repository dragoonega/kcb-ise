#!/usr/bin/python

import pandas as pd

def team():
    # Extract
    participant = pd.read_csv('csv/Dim_Participant.csv')
    team = pd.read_csv('csv/Dim_Team.csv')
    city = pd.read_csv('csv/Dim_City.csv')
    region = pd.read_csv('csv/Dim_Region.csv')
    instance = pd.read_csv('csv/Dim_Instance.csv')

    # Transform
    participant = participant.rename(columns={'category': 'team_category'})
    team = team.rename(columns={'name':'team_name'})
    city = city.rename(columns={'name': 'city_name', 'region': 'region_id'})
    region = region.rename(columns={'id': 'region_id', 'name': 'region_name'})
    instance = instance.rename(columns={'name':'instance_name'})

    participant = participant[['id', 'team_category']]
    team = pd.merge(team, participant, how='inner', on='id')

    city = pd.merge(city, region, how='inner', on='region_id')
    city = city[['id', 'city_name', 'region_name']]
    
    instance = instance[['id', 'instance_name']]

    team = pd.merge(team, city, on=['id'], how='inner')
    team = pd.merge(team, instance, on=['id'], how='inner')

    # Load
    team.to_csv('csv/Fact_Team.csv',index=False)