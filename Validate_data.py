#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 09:54:08 2018

@author: Prabz
"""
import datetime
import importlib
import pandas as pd

import read_Stock_Data as rd
importlib.reload(rd)  

start_date = datetime.date(2012,1,1)
end_date = datetime.date.today()     #date.today() datetime.date(2013,12,31)
  

def check_null():
    df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
    list_nse50 = df_nse50.index.tolist()                            #Array of Stcok symbols

    df_Stock = rd.get_data(list_nse50[0:51],start_date,end_date)        #Extract Data from a given date to past x no of traded days
    df_Stock = df_Stock.dropna(how='all') 
    
    null_count =  df_Stock.isnull().sum(axis=0)
    print(null_count)

def sort_file():
    df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
    list_nse50 = df_nse50.index.tolist()

           #Extract Data from a given date to past x no of traded days
    for symbol in list_nse50[0:51]:
        df_temp = pd.read_csv(rd.list_to_path(symbol), index_col='Date',
                parse_dates=True,dayfirst=True, na_values=['nan'])    #Extract date , series and price
        df_temp = df_temp.reset_index().drop_duplicates(subset='Date', keep='last').set_index('Date')
        df_temp.sort_index(ascending=True, inplace=True)                                       #Remove any blank records
        df_temp.to_csv("/Volumes/2/PyD/nsedb/{}.csv".format(str(symbol)))         
    
def test_run():
    check_null()
    sort_file()

if __name__ == "__main__":
    test_run()