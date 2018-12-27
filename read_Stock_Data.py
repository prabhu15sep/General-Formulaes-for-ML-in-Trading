

"""Reads the Stocks symbols and return Dataframe"""

import os
import pandas as pd

def list_to_path(list_name, base_dir="/Volumes/2/PyD/nsedb"):    #"D:\\ppadayac\\PraChin\\db\\new"  "/Volumes/2/PyD/nsedb"
    """Return CSV file p  ath given ticker symbol."""
    return os.path.join(base_dir, "{}.csv".format(str(list_name)))
    
def get_list(list_name,ind_key):
    df = pd.read_csv(list_to_path(list_name), index_col=ind_key,
            parse_dates=True, usecols=['Symbol', 'Series','ISIN Code'], na_values=['nan']) # Extract Symbol,EQ series and Stock Code
    return df

def get_data(symbols,start_date,end_date):
    """Read stock data (adjusted close) for given symbols from CSV files."""
    dates = pd.date_range(start = start_date ,end=end_date)                                      #Extract data of 90 days
    df = pd.DataFrame(index=dates)
    for symbol in symbols:
        df_temp = pd.read_csv(list_to_path(symbol), index_col='Date',
                parse_dates=True,dayfirst=True, usecols=['Date', 'Adj Close'], na_values=['nan'])    #Extract date , series and price
        df_temp = df_temp.rename(columns={'Adj Close': symbol})                        #Rename Column name with Symbol
        #df_temp = df_temp.dropna()                                        #Remove any blank records
        
        if symbol == 'NIFTY':                                             
            join_typ = 'inner'
        else:
            join_typ = 'left'
        
        df = df.join(df_temp,how=join_typ)
 #      df = df.dropna(subset=[symbol])                                                  #Remove any blank records
    return df                                                                  #Return last 20 traded days record

    