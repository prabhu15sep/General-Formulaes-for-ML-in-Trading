import pandas as pd


def stockAvg(df_Stock,df_result_set):
    df_temp = pd.DataFrame(df_Stock.mean().round(3))    #Calculating Mean of all Stocks
    df_temp.columns =  ['Mean']                         #Naming the column as 'Mean'
    return df_result_set.join(df_temp)                  #Joining Mean data to the given set

def stockRet(df_Stock,df_result_set,day):
    df_temp =(df_Stock/df_Stock.shift(day)) -1                       #Calculating Return of all Stocks
    df_temp = df_temp.round(3)
    df_temp = (df_temp[-1:]).T                                       #Transpose
    df_temp.columns =  ['Return']                                    #Naming the column as 'Return'
    return df_result_set.join(df_temp)                               #Joining Mean data to the given set

def modstockRet(df_Stock,day):
    df_temp =(df_Stock/df_Stock.shift(day)) -1                       #Calculating Return of all Stocks
    df_temp = df_temp.tail(50).abs()                                     #Transpose
    return df_temp

def RSI(df_Stock,df_result_set):
    df_temp = df_Stock.diff()                                       # Get the difference in price from previous step
    df_temp = df_temp[1:]                                           # Get rid of the first row Nan

    up, down = df_temp.copy(), df_temp.copy()
    up[up < 0] = 0                                                  # Make the positive gains (up) 
    down[down > 0] = 0                                              # and negative gains (down) Series
    
    roll_up2 = pd.DataFrame(up.mean().round(3))                     # Calculate the SMA
    roll_down2 = pd.DataFrame(down.mean().round(3).abs())
    
    RS = roll_up2.div(roll_down2)                       
    RSI = 100.0 - (100.0 / (1.0 + RS))                              # Calculate the RSI based on SMA
    RSI.columns =  ['RSI']  
    return df_result_set.join(RSI)