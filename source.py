#!/usr/bin/python

import pandas as pd

def source():
    # Extract
    participant = pd.read_csv('csv/Dim_Participant.csv')
    source = pd.read_csv('csv/Dim_Source.csv')

    # Transform
    participant = participant.rename(columns={'full_name': 'participant_name', 'category': 'participant_category'})
    source = source.rename(columns={'source': 'source_name'})

    participant = participant[['id', 'participant_name', 'participant_category']]

    source = pd.merge(participant, source, on=['id'], how='inner')

    # Load
    source.to_csv('csv/Fact_Source.csv', index=False)