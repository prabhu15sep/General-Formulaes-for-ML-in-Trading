#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 15 22:59:09 2018

@author: Prabz
"""
import pandas as pd
import datetime
import importlib
import numpy as np
from collections import OrderedDict

import read_Stock_Data as rd
importlib.reload(rd)                                                #Reload file to cater for any recent changes in the file

import stock_formulae as frm
importlib.reload(frm) 

import common as cmn
importlib.reload(cmn)

import rolling_formulae as rf
importlib.reload(rf)

base_dir="/Volumes/2/Files/PraChin/SCVM/Sangachatvam"


def get_result(df_col,k):
    df_col.dropna(how='all',inplace=True)
    
    for count, elem in enumerate(df_col.values):
        #if count == 0:
        #with open(rd.list_to_path("result",base_dir), 'r') as f:
        #    data = f.read() 
            
        #df_csv = pd.DataFrame(elem,index=[df_col.name],columns=[df_col.index[count]])
        df_csv = df_price[(df_price.index >= df_col.index[count])][df_col.name]
        stop_val = df_stop.loc[df_col.index[count],df_col.name]
        if elem == 'B':
            list_target_P = df_csv[df_csv > ((1+1.75*stop_val)*df_csv.loc[df_col.index[count]])]
            list_target_L = df_csv[df_csv < ((1-1.1*stop_val)*df_csv.loc[df_col.index[count]])]
            
        if elem == 'S':
            list_target_P = df_csv[df_csv < ((1-1.5*stop_val)*df_csv.loc[df_col.index[count]])]
            list_target_L = df_csv[df_csv > ((1+1.1*stop_val)*df_csv.loc[df_col.index[count]])]
        
        P_was_empty,L_was_empty = False,False
        if list_target_P.empty : 
            list_target_P = pd.Series(df_csv.iloc[[-1]])
            P_was_empty = True
        if list_target_L.empty : 
            list_target_L = pd.Series(df_csv.iloc[[-1]])
            L_was_empty = True
            
        list_target_F = list_target_P.index[0] 
        result = 'P'
        if list_target_L.index[0] < list_target_P.index[0] :
            list_target_F = list_target_L.index[0]
            result = 'L'
            
        df_csv = df_csv[(df_csv.index >= df_col.index[count]) & (df_csv.index <= list_target_F )]
        
        n1,n2=0,0
        if df_csv.index[0] != start_date :n1= 1
        if df_csv.index[-1] != end_date :n2= 1
        
        symb_start_index_loc = df_price.index.get_loc(df_csv.index[0]) - n1
        symb_end_index_loc = df_price.index.get_loc(df_csv.index[-1]) + n2    
            
        df_csv = df_price.iloc[symb_start_index_loc:symb_end_index_loc+1][df_col.name]
        df_csv = df_csv.to_frame().T
        df_csv.index.name = elem
        
        df_csv.loc['Signal'] = ''
        df_csv.iloc[1,n1] = elem
        if(((result == 'P') and (not(P_was_empty))) or ((result == 'L') and (not(L_was_empty)))):
            df_csv.iloc[1,-(n2+1)] = result
        df_csv.loc['EOL'] = ''
        with open(rd.list_to_path("result",base_dir), 'a+') as f:
            #if(not(data and (df_col.name==k))):
            df_csv.to_csv(f)

df_price = pd.read_csv(rd.list_to_path("B1_StockPrice_30.12.2016", base_dir="/Volumes/2/Files/PraChin/SCVM/Sangachatvam"),
            parse_dates=True,dayfirst=True,index_col = 0, na_values=['nan']) # Extract Symbol,EQ series and Stock Code
df_price.sort_index(inplace=True)
start_date = df_price.index[0]
end_date = df_price.index[-1]


df_stop = pd.read_csv(rd.list_to_path("B8_ra_15DHL_30.12.2016", base_dir="/Volumes/2/Files/PraChin/SCVM/Sangachatvam"),
            parse_dates=True,index_col = 0, dayfirst=True,na_values=['nan'])

df = pd.read_csv(rd.list_to_path("B19_Cntr_Breakout_30.12.2016", base_dir="/Volumes/2/Files/PraChin/SCVM/Sangachatvam"),
            parse_dates=True,index_col = 0,dayfirst=True, na_values=['nan']) # Extract Symbol,EQ series and Stock Code
df.index.name = 'Date'

df.dropna(how='all',inplace=True)
df.dropna(how='all',axis=1,inplace=True)

dfT = df.T

stacked = dfT.stack()
stacked = stacked.reset_index()

stacked.sort_values(['Date'],inplace=True)

stock_names = stacked.iloc[:,0:1].values
stock_names = [",".join(item) for item in stock_names.astype(str)]

unqiue_stock_names = OrderedDict.fromkeys(stock_names).keys()

unqiue_stock_names = [*unqiue_stock_names]
df = df[unqiue_stock_names]
df.sort_index(inplace=True)
first_stock = unqiue_stock_names[0]
#df.apply(lambda x: get_result(x,first_stock))
for col in df.columns:
    get_result(df[col],first_stock)
#print(df)