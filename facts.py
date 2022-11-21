#!/usr/bin/python

import petl as etl

# Participant Fact Table
def participant():
    # Participant Name
    participant = etl.fromcsv('csv/Dim_Participant.csv')
    dim = etl.cut(participant, 'id', 'full_name')
    fact = etl.cut(participant, 'id')

    fact = etl.join(fact, dim)
    fact = etl.rename(fact, 'full_name', 'participant_name')

    # City Name
    city = etl.fromcsv('csv/Dim_City.csv')
    dim = etl.cut(city, 'id', 'name')
    
    fact = etl.join(fact, dim)
    fact = etl.rename(fact, 'name', 'city_name')

    # Region
    # belom ada dimensi region

    # Instance Name
    instance = etl.fromcsv('csv/Dim_Instance.csv')
    dim = etl.cut(instance, 'id', 'name')
    
    fact = etl.join(fact, dim)
    fact = etl.rename(fact, 'name', 'instance_name')

    # Team Name
    team = etl.fromcsv('csv/Dim_Team.csv')
    dim = etl.cut(team, 'id', 'name')

    fact = etl.join(fact, dim)
    fact = etl.rename(fact, 'name', 'team_name')

    etl.tocsv(fact, 'csv/Fact_Participant.csv')