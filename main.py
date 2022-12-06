#!/usr/bin/python

import os
from participant import participant
from sales import sales
from team import team
from source import source
from extract_dimtable import etl_dimensiontable
from onetimehistory import onetimehistory


def pre():
    if os.path.exists('csv/Dim_City.csv'):
        os.remove('csv/Dim_City.csv')
    
    if os.path.exists('csv/Dim_Participant.csv'):
        os.remove('csv/Dim_Participant.csv')
    
    if os.path.exists('csv/Dim_Instance.csv'):
        os.remove('csv/Dim_Instance.csv')
    
    if os.path.exists('csv/Dim_Payment.csv'):
        os.remove('csv/Dim_Payment.csv')
        
    if os.path.exists('csv/Dim_Time.csv'):
        os.remove('csv/Dim_Time.csv')
        
    if os.path.exists('csv/Dim_Source.csv'):
        os.remove('csv/Dim_Source.csv')
        
    if os.path.exists('csv/Dim_Team.csv'):
        os.remove('csv/Dim_Team.csv')
        
    if os.path.exists('csv/Dim_Region.csv'):
        os.remove('csv/Dim_Region.csv')
        
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
    etl_dimensiontable()
    onetimehistory()
    participant()
    sales()
    team()
    source()

if __name__ == '__main__':
    main()