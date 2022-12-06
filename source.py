#!/usr/bin/python

import pandas as pd
from onetimehistory import conndb2,conn2

def source():
    conndb2()
    # Extract
    conn2.execute('SET IDENTITY_INSERT Fact_Source ON;')
    participant = pd.read_csv('csv/Dim_Participant.csv')
    source = pd.read_csv('csv/Dim_Source.csv')

    # Transform
    participant = participant.rename(columns={'full_name': 'participant_name', 'category': 'participant_category'})
    source = source.rename(columns={'source': 'source_name'})

    participant = participant[['id', 'participant_name', 'participant_category']]

    source = pd.merge(participant, source, on=['id'], how='inner')

    # Load
    source.to_sql('Fact_Source', conn2, if_exists='append', index=False)
    source.to_csv('csv/Fact_Source.csv', index=False)
    conn2.execute('SET IDENTITY_INSERT Fact_Source OFF;')
    
source()