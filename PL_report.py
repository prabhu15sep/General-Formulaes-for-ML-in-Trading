# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 15:21:08 2018

@author: ppadayac
"""
import os
import numpy as np
import datetime
import importlib
import read_Stock_Data as rd
importlib.reload(rd)  
import pandas as pd
from openpyxl import load_workbook
excel_file = '/Volumes/2/Files/PraChin/SCVM/InvestValue.xlsx'  #/Volumes/2/Files/PraChin/   D:\\ppadayac\\PraChin\\InvestValue.xlsx

movies_sheet1 = pd.read_excel(excel_file, sheet_name=0)
movies_sheet1['Date'] = pd.to_datetime(movies_sheet1['Date'],dayfirst=True)
Dates = movies_sheet1['Date'].tolist()

start_date = datetime.date(2018,1,1)
end_date = datetime.date.today()     #date.today() datetime.date(2013,12,31)


df_nse50 = rd.get_list('ind_nifty50list', 'Symbol')                   #Extract list of Top Nse50 Stcoks
list_nse50 = df_nse50.index.tolist() 
df_Stock = rd.get_data(list_nse50[1:51],start_date,end_date)  

columns_buy = movies_sheet1['Buy'].tolist()
columns_sell = movies_sheet1['Sell'].tolist()
buy_size = movies_sheet1['BuySize'].tolist()
sell_size = movies_sheet1['Sell Size'].tolist()

date_range = pd.date_range(start= Dates[1],end=Dates[2])    
new_df = df_Stock.loc[np.concatenate((np.array([Dates[0]]),date_range.tolist()),axis=0),np.concatenate((columns_buy,columns_sell),axis=0)]
new_df = new_df.dropna(how='all')
new_df = new_df.dropna(how='all',axis = 1)
one = np.concatenate((buy_size,sell_size),axis=0)
one = one[~(np.isnan(one))]
new_df = new_df * one
new_df['Total asset'] = new_df.sum(axis=1)
new_df['Profit'] = new_df['Total asset'] - new_df['Total asset'][0]
new_df['Value'] = new_df['Total asset']*0.06
new_df.insert( len(columns_buy),'Sell', np.nan)


new_df.index = new_df.index.strftime('%d/%m/%Y')
movies_sheet1['Date'] = movies_sheet1['Date'].dt.strftime('%d/%m/%Y')
movies_sheet1['Date'] = np.where(movies_sheet1['Date'] == 'NaT','',movies_sheet1['Date'])
##movies_sheet1.iloc[3:5,0:1] = ''
xl_file = pd.ExcelWriter(excel_file)
movies_sheet1.to_excel(xl_file, sheet_name='Investment',index=False)
new_df.to_excel(xl_file, sheet_name='Profit Summary')

xl_file.save()

print(new_df)