#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 13:32:11 2018

@author: Prabz
"""
import pandas as pd
import glob
import datetime
import importlib
import os
import read_Stock_Data as rd
importlib.reload(rd) 

df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
list_nse50 = df_nse50.index.tolist()
end_date = datetime.date.today()
yahoo_files_path = "/Volumes/2/PyD/Yahoo"
       #Extract Data from a given date to past x no of traded days
for symbol in list_nse50[0:51]:
    list_set =  glob.glob("{}/{}*.csv".format(yahoo_files_path,symbol))
    list_set.sort(key=os.path.getmtime)
    df_symbol = pd.read_csv(rd.list_to_path(symbol), index_col='Date',
        parse_dates=True,dayfirst=True, na_values=['nan'])    #Extract date , series and price

    for file in list_set :
        df_temp = pd.read_csv(file, index_col='Date',parse_dates=True,
                   usecols=['Date', 'Close','Adj Close'],dayfirst=True, na_values=['nan'])    #Extract date , series and price
        df_temp = df_temp.rename(columns={'Close': 'Y-Close'}) 
        df_temp = df_temp.dropna(how='all')
        df_symbol = df_temp.combine_first(df_symbol)
        
    if len(list_set) > 0:
        df_symbol.sort_index(ascending=True, inplace=True)                                       #Remove any blank records
        df_symbol.dropna(subset=['Close'],inplace=True)
        cols = df_symbol.columns.tolist()
        cols.insert(0, cols.pop(cols.index('Adj Close')))
        cols.insert(1, cols.pop(cols.index('Close')))
        cols.insert(2, cols.pop(cols.index('Y-Close')))
        df_symbol = df_symbol.reindex(columns= cols)
        df_symbol.to_csv("/Volumes/2/PyD/nsedb/{}.csv".format(str(symbol)))  
    
    for file in list_set :
        os.rename(file, "{}.merged".format(file))