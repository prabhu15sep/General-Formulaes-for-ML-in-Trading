# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 17:39:40 2018
Load the Database with NSE Fifty stocks
@author: ppadayac
"""
import pandas as pd
import importlib
from datetime import date
from nsepy import get_history
import numpy as np

import read_Stock_Data as rd
importlib.reload(rd)  

df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
list_nse50 = df_nse50.index.tolist() 

start_date = date(2018,9,1)
end_date = date(2018,11,22)                 #date.today()#date(2012,12,31)
rec_count = {}
sbin = get_history(symbol="NIFTY",start=start_date,end=end_date,index=True)
rec_count['NIFTY'] = len(sbin.index)
cols = sbin.columns.tolist()
cols.sort()
cols.insert(0, cols.pop(cols.index('Close')))
sbin = sbin.reindex(columns= cols)
sbin.insert( 0,'Adj Close', np.nan)
sbin.insert( 2,'Y-Close', np.nan)
#sbin.to_csv("/Volumes/2/PyD/nsedb/NIFTY.csv") 
with open('/Volumes/2/PyD/nsedb/NIFTY.csv', 'a') as f:
    sbin.to_csv(f, header=False)
    
#list_nse50 = ['EICHERMOT']

for symbl in list_nse50[1:]:
    sbin = get_history(symbol=symbl, start=start_date,end=end_date)
    rec_count[symbl] = len(sbin.index)
    cols = sbin.columns.tolist()
    cols.sort()
    cols.insert(0, cols.pop(cols.index('Close')))
    sbin = sbin.reindex(columns= cols)
    sbin.insert( 0,'Adj Close', np.nan)
    sbin.insert( 2,'Y-Close', np.nan)
#    sbin.to_csv("/Volumes/2/PyD/nsedb/{}.csv".format(str(symbl)))    #D:\\ppadayac\\PraChin\\db\\new\\
    with open("/Volumes/2/PyD/nsedb/{}.csv".format(str(symbl)), 'a') as f:
        sbin.to_csv(f, header=False)
    
print(rec_count)
trading_days_cnt = np.busday_count( start_date, end_date )
print("Total Trading Days: {}".format(trading_days_cnt))
#sbin.to_csv("D:\\ppadayac\\PraChin\\db\\SBIN.csv")