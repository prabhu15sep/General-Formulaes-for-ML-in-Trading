import pandas as pd
import datetime
import importlib
import numpy as np


import read_Stock_Data as rd
importlib.reload(rd)                                                #Reload file to cater for any recent changes in the file

import stock_formulae as frm
importlib.reload(frm) 




def test_run():
    ######Reading Data###################
    df_nse50 = rd.get_list('nse_Top50', 'Symbol')                   #Extract list of Top Nse50 Stcoks
    list_nse50 = df_nse50.index.tolist()                            #Array of Stcok symbols
    
    df_Stock = rd.get_data(list_nse50[0:50],datetime.date.today(),365)        #Extract Data from a given date to past x no of traded days
    df_Stock.index = df_Stock.index.strftime('%d.%m.%Y')            #Formate date to DD/MM/YYYY formate
    #print(df_Stock)

    ######Creating Filter################
    df_filter = (df_Stock[-1:]).T                                   #Transposing the matrix to (rows as stocks and 1st column as last close price) for given date
    str_trade_date = ''.join(df_filter.columns.tolist())            #Extrating the 1st column name
    col_A = "A~CP-{}".format(str_trade_date)          
              
    df_filter = df_filter.rename(columns={str_trade_date: col_A}) 
    
    df_filter = (frm.stockAvg(df_Stock.tail(50),df_filter)).round(2)         # 1st Filter of calculating 50 Day Mean by less than 5 percent
    df_filter = df_filter.rename(columns={'Mean': 'B~50DA'})

    
    df_filter = (frm.stockAvg(df_Stock.tail(200),df_filter)).round(2)        # 2nd Filter of calculating 200 Day Mean by less than 5 percent
    df_filter = df_filter.rename(columns={'Mean': 'C~200DA'}) 
    
    df_filter = frm.stockRet(df_Stock,df_filter,10)                          # 3rd Filter of calculating 10 Day Return
    df_filter = df_filter.rename(columns={'Return': 'D~10DR'}) 
    
    df_filter_temp = frm.modstockRet(df_Stock,1)                             # 4th Filter of calculating Absolute Return of 50 days
    df_filter = frm.stockAvg(df_filter_temp,df_filter) 
    df_filter = df_filter.rename(columns={'Mean': 'E~AR50D'})           
    
    df_filter = (frm.RSI(df_Stock.tail(15),df_filter)).round(2)              # 5th Filter of calculating RSI of 14 days    
    df_filter = df_filter.rename(columns={'RSI': 'F~RSI'}) 
    
    #####################Setting Criteria#######
    
    ########################Citeria 1 to Buy###########
    df_filter['A<(B/C) '] = np.where(((df_filter[col_A] <= df_filter['B~50DA']) & \
                                      (df_filter[col_A] >= (df_filter['B~50DA'])*0.95)) \
                                    |((df_filter[col_A] <= df_filter['C~200DA']) & \
                                      (df_filter[col_A] >= (df_filter['C~200DA'])*0.95)), 1 , 0)
    
    ########################Criteria 2 to Buy##########
    df_filter['D<-8%'] = np.where(df_filter['D~10DR'] < -0.08, 1 , 0)
    
    ########################Criteria 3 to Buy ##########
    df_filter['E<1.5%'] = np.where(df_filter['E~AR50D'] < 0.015, 1 , 0)   

    ########################Criteria 4 to Buy ##########
    df_filter['F<30%'] = np.where(df_filter['F~RSI'] < 30, 1 , 0)   
    
    ########################Citeria to Sell###########
    df_filter['A>(B/C) '] = np.where(((df_filter[col_A] >= df_filter['B~50DA']) & \
                                      (df_filter[col_A] <= (df_filter['B~50DA'])*1.05)) \
                                    |((df_filter[col_A] >= df_filter['C~200DA']) & \
                                      (df_filter[col_A] <= (df_filter['C~200DA'])*1.05)), 1 , 0)
    
    ########################Criteria 2 to Sell ##########
    df_filter['D>8%'] = np.where(df_filter['D~10DR'] > 0.08, 1 , 0)
    
    ########################Criteria 3 to Sell##########
    df_filter['E>1.5%'] = np.where(df_filter['E~AR50D'] > 0.015, 1 , 0) 
    
    ########################Criteria 4 to Sell ##########
    df_filter['F>70%'] = np.where(df_filter['F~RSI'] > 70, 1 , 0) 
    
    ####################Ranking to Buy/Sell#####
    df_filter['BuyS']   =  df_filter['A<(B/C) '] + df_filter['D<-8%'] + df_filter['E<1.5%'] + df_filter['F<30%']
    
    df_filter['SellS']  =  df_filter['A>(B/C) '] + df_filter['D>8%'] + df_filter['E>1.5%'] + df_filter['F>70%']
    
    pd.set_option('display.max_rows', 500)
    print(df_filter)  
    df_filter.to_csv("TradeCall_{}.csv".format(str_trade_date))
    
if __name__ == "__main__":
    test_run()