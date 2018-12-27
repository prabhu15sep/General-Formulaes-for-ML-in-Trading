#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 19:47:14 2018

@author: Prabz
"""
import os
import importlib
import numpy as np
import pandas as pd
import common as cmn
importlib.reload(cmn)

def get_rolling_mean(df, window):
    """Return rolling mean of given values, using specified window size."""
    #return pd.rolling_mean(values, window=window)
    columns = df.columns.values.tolist()
    df_temp = df.copy()
    for x in columns:
        df_temp[x] = pd.Series(df[x]).rolling(window=window).mean()
        
    return df_temp

def get_rolling_std(df, window):
    """Return rolling standard deviation of given values, using specified window size."""
    # TODO: Compute and return rolling standard deviation
    #return pd.rolling_std(values,window=window)
    columns = df.columns.values.tolist()
    df_temp = df.copy()
    for x in columns:
        df_temp[x] = pd.Series(df[x]).rolling(window=window).std()
        
    return df_temp

def compute_daily_returns(df,n=1):
    """Compute and return the daily return values."""
    df_daily_ret = (df/df.shift(n)) -1
    # Note: Returned DataFrame must have the same number of rows
    #df_daily_ret.ix[0,:] = 0
    df_daily_ret.loc[df_daily_ret.index[0:(n-1)],:]= 0
    return df_daily_ret.abs()

def get_rolling_nAvg(df,window,n1,n2,n3):
    df_nAvg = df.rolling(window=window, min_periods=window, center=False).apply \
                            (lambda x: cmn.find_nlargest_avg(x,window,n1,n2,n3))
    return df_nAvg

def get_rolling_small(df,window,n):
    df_nSmall = df.rolling(window=window, min_periods=window, center=False).apply \
                            (lambda x: cmn.find_nsmallest(x,window,n))
    return df_nSmall

def get_rolling_VMAP(df,window,n):
    df_VMAP = df.rolling(window=window, min_periods=window, center=False).apply \
                            (lambda x: cmn.findVWAP(x,window,n))
    return df_VMAP

def get_rolling_nDHL(df,window,n):
    df_nDHL = df.rolling(window=window, min_periods=window, center=False).apply \
                            (lambda x: cmn.find_nDHL(x,window,n))
    return df_nDHL

def get_Rolling_RSI(df,window_length):
    df_temp = df.diff()                                       # Get the difference in price from previous step
    df_temp = df_temp[1:]                                           # Get rid of the first row Nan

    up, down = df_temp.copy(), df_temp.copy()
    up[up < 0] = 0                                                  # Make the positive gains (up) 
    down[down > 0] = 0                                              # and negative gains (down) Series
    
    # Calculate the EWMA
    #roll_up1 = pd.stats.moments.ewma(up, window_length)
    #roll_down1 = pd.stats.moments.ewma(down.abs(), window_length)

    # Calculate the RSI based on EWMA
    #RS1 = roll_up1 / roll_down1
    #RSI1 = 100.0 - (100.0 / (1.0 + RS1))
    
    # Calculate the SMA
    roll_up2 = get_rolling_mean(up,window_length)
    roll_down2 = get_rolling_mean(down.abs(), window_length)
    
    # Calculate the RSI based on SMA
    RS2 = roll_up2.div(roll_down2) 
    RSI2 = 100.0 - (100.0 / (1.0 + RS2))
    
    return RSI2
