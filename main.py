#!/usr/bin/python

import os
from participant import participant
from sales import sales
from team import team
from source import source

def pre():
    if os.path.exists('csv/Fact_Participant.csv'):
        os.remove('csv/Fact_Participant.csv')
    
    if os.path.exists('csv/Fact_Sales.csv'):
        os.remove('csv/Fact_Sales.csv')
    
    if os.path.exists('csv/Fact_Team.csv'):
        os.remove('csv/Fact_Team.csv')
    
    if os.path.exists('csv/Fact_Source.csv'):
        os.remove('csv/Fact_Source.csv')

def main():
    pre()
    participant()
    sales()
    team()
    source()

if __name__ == '__main__':
    main()