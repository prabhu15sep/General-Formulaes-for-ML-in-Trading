#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 25 19:25:49 2018

@author: Prabz
"""

import pandas as pd
import datetime
import importlib
import numpy as np


import read_Stock_Data as rd
importlib.reload(rd)                                                #Reload file to cater for any recent changes in the file

import stock_formulae as frm
importlib.reload(frm) 

import common as cmn
importlib.reload(cmn)

import rolling_formulae as rf
importlib.reload(rf)



def test_run():
    ######Reading Data###################
    df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
    list_nse50 = df_nse50.index.tolist()                            #Array of Stcok symbols
    n = 100
    csv = "CSV"
    excel = "EXCEL"
    start_date = datetime.date(2012,1,1)
    end_date = datetime.date(2017,12,31)    #date.today() datetime.date(2013,12,31)
    
    df_Stock = rd.get_data(list_nse50[0:51],start_date,end_date)        #Extract Data from a given date to past x no of traded days
    df_Stock = df_Stock.dropna(how='all') 
    df_Stock.fillna(method='ffill', inplace=True)
        #Formate date to DD/MM/YYYY formate
    df_Stock.index.name = 'Date'
    str_trade_date = ''.join(df_Stock.iloc[-1:,:0].index.strftime('%d.%m.%Y'))
    writer = cmn.to_file(df_Stock.iloc[-n:,:].sort_index(ascending=False, inplace=False) ,"B1_StockPrice",csv,str_trade_date)
   
    day_10_returns = rf.compute_daily_returns(df_Stock,10)
    small30_10D = rf.get_rolling_small(day_10_returns.iloc[-1200-n:,:],1200,360)
    large30_10D = rf.get_rolling_small(day_10_returns.iloc[-1200-n:,:],1200,840)
    small30_10D.dropna(how='all',inplace=True)
    large30_10D.dropna(how='all',inplace=True)
    writer = cmn.to_file(day_10_returns.sort_index(ascending=False, inplace=False),"B12_10D",csv,str_trade_date)
    writer = cmn.to_file(small30_10D.sort_index(ascending=False, inplace=False),"B2_small30_10D",csv,str_trade_date)
    writer = cmn.to_file(large30_10D.sort_index(ascending=False, inplace=False),"B3_large30_10D",csv,str_trade_date)
     
    day_15_returns = rf.compute_daily_returns(df_Stock,15)
    small30_15D = rf.get_rolling_small(day_15_returns.iloc[-1200-n:,:],1200,360)
    large30_15D = rf.get_rolling_small(day_15_returns.iloc[-1200-n:,:],1200,840)
    small30_15D.dropna(how='all',inplace=True)
    large30_15D.dropna(how='all',inplace=True)
    writer = cmn.to_file(day_15_returns.sort_index(ascending=False, inplace=False),"B31_10D",csv,str_trade_date)
    writer = cmn.to_file(small30_15D.sort_index(ascending=False, inplace=False),"B4_small30_15D",csv,str_trade_date)
    writer = cmn.to_file(large30_15D.sort_index(ascending=False, inplace=False),"B5_large30_15D",csv,str_trade_date)
 
    day_5_returns = rf.compute_daily_returns(df_Stock,5)
    small30_5D = rf.get_rolling_small(day_5_returns.iloc[-1200-n:,:],1200,360)
    large30_5D = rf.get_rolling_small(day_5_returns.iloc[-1200-n:,:],1200,840)
    small30_5D.dropna(how='all',inplace=True)
    large30_5D.dropna(how='all',inplace=True)
    writer = cmn.to_file(day_5_returns.sort_index(ascending=False, inplace=False),"B51_5D",csv,str_trade_date)
    writer = cmn.to_file(small30_5D.sort_index(ascending=False, inplace=False),"B6_small30_5D",csv,str_trade_date)
    writer = cmn.to_file(large30_5D.sort_index(ascending=False, inplace=False),"B7_large30_5D",csv,str_trade_date)
     
    ra_15DHL = rf.get_rolling_nDHL(df_Stock.iloc[-1215-n:,:],window=15,n=15)
    small30_15DHL = rf.get_rolling_small(ra_15DHL.iloc[-1200-n:,:],1200,360)
    large30_15DHL = rf.get_rolling_small(ra_15DHL.iloc[-1200-n:,:],1200,840)
    small30_15DHL.dropna(how='all',inplace=True)
    large30_15DHL.dropna(how='all',inplace=True)
    writer = cmn.to_file(ra_15DHL.sort_index(ascending=False, inplace=False),"B8_ra_15DHL",csv,str_trade_date)
    writer = cmn.to_file(small30_15DHL.sort_index(ascending=False, inplace=False),"B9_small30_15DHL",csv,str_trade_date)
    writer = cmn.to_file(large30_15DHL.sort_index(ascending=False, inplace=False),"B10_large30_15DHL",csv,str_trade_date)

    ra_10DHL = rf.get_rolling_nDHL(df_Stock.iloc[-1210-n:,:],window=10,n=10)
    small30_10DHL = rf.get_rolling_small(ra_10DHL.iloc[-1200-n:,:],1200,360)
    large30_10DHL = rf.get_rolling_small(ra_10DHL.iloc[-1200-n:,:],1200,840)
    small30_10DHL.dropna(how='all',inplace=True)
    large30_10DHL.dropna(how='all',inplace=True)
    writer = cmn.to_file(ra_10DHL.sort_index(ascending=False, inplace=False),"B11_ra_10DHL",csv,str_trade_date)
    writer = cmn.to_file(small30_10DHL.sort_index(ascending=False, inplace=False),"B12_small30_10DHL",csv,str_trade_date)
    writer = cmn.to_file(large30_10DHL.sort_index(ascending=False, inplace=False),"B13_large30_10DHL",csv,str_trade_date)

    ra_5DHL = rf.get_rolling_nDHL(df_Stock.iloc[-1205-n:,:],window=5,n=5)
    small30_5DHL = rf.get_rolling_small(ra_5DHL.iloc[-1200-n:,:],1200,360)
    large30_5DHL = rf.get_rolling_small(ra_5DHL.iloc[-1200-n:,:],1200,840)
    small30_5DHL.dropna(how='all',inplace=True)
    large30_5DHL.dropna(how='all',inplace=True)
    writer = cmn.to_file(ra_5DHL.sort_index(ascending=False, inplace=False),"B14_ra_5DHL",csv,str_trade_date)
    writer = cmn.to_file(small30_5DHL.sort_index(ascending=False, inplace=False),"B15_small30_5DHL",csv,str_trade_date)
    writer = cmn.to_file(large30_5DHL.sort_index(ascending=False, inplace=False),"B16_large30_5DHL",csv,str_trade_date)

    
    large_15D = rf.get_rolling_small(df_Stock.iloc[-15-n:,:],15,14)
    large_15D.dropna(how='all',inplace=True)
    small_15D = rf.get_rolling_small(df_Stock.iloc[-15-n:,:],15,1)
    small_15D.dropna(how='all',inplace=True)
    writer = cmn.to_file(large_15D.sort_index(ascending=False, inplace=False),"B17_large_15D",csv,str_trade_date)
    writer = cmn.to_file(small_15D.sort_index(ascending=False, inplace=False),"B18_small_15D",csv,str_trade_date)

    
    array_cntr = np.where(small30_5DHL.isnull() ,np.nan,
                      np.where((day_10_returns.iloc[-1-n:,:] < small30_10D) & 
                               (day_15_returns.iloc[-1-n:,:] < small30_15D) &
                               (day_5_returns.iloc[-1-n:,:] < small30_5D) &
                               (df_Stock.iloc[-1-n:,:] > large_15D),'B',
                      np.where((day_10_returns.iloc[-1-n:,:] > large30_10D) & 
                               (day_15_returns.iloc[-1-n:,:] > large30_15D) &
                               (day_5_returns.iloc[-1-n:,:] > large30_5D) &
                               (df_Stock.iloc[-1-n:,:] < small_15D),'S',np.nan)))
    indx = small30_5DHL.index.values
    Cntr_Breakout = pd.DataFrame(array_cntr,index=indx ,columns=list_nse50)
    writer = cmn.to_file(Cntr_Breakout.sort_index(ascending=False, inplace=False),"B19_Cntr_Breakout",csv,str_trade_date)
    
    
if __name__ == "__main__":
    test_run()
    
    
