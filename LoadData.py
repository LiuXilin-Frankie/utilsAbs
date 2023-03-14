import datetime
import re
import time
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import pickle
import joblib
import sys
import random
import threading
import shutil
import pyarrow.parquet as pq
from joblib import Parallel, delayed
import multiprocessing

def LoadEODPrices(Fundamentalpath,DataHousepath):
    """
    获取每个交易日交易转债的日频汇总
    同时将可转债转换股票代码merge到该表单中

    Input:
        - Fundamentalpath: 存储基本面信息数据的路径
        - DataHousepath: 日频汇总存储路径
    Output:
        - EODPrices: 日频汇总并加入可转债对应正股的表单
    """
    #获取基本面信息数据
    CbondStockInfoAll = pd.read_csv(Fundamentalpath+'/FundamentalData/CbondStockInfoAll.csv')
    CbondStockInfoAll = CbondStockInfoAll[['secucode','convert_stock_code','transfer_start_date']].rename(columns={'secucode':'code'})
    CbondStockInfoAll['transfer_start_date'] = CbondStockInfoAll.transfer_start_date.apply(lambda x: str(x)[:4]+str(x)[5:7]+str(x)[8:10])
    #获取我们根据tick数据生成的EODPrices信息
    EODPrices = pd.read_parquet(DataHousepath+'CBond/EODPrices.parquet')
    EODPrices['date'] = EODPrices['date'].astype(str)
    #过滤掉交易时无法选择转股的可转债
    EODPrices = pd.merge(EODPrices,CbondStockInfoAll,on=['code'],how='left')
    EODPrices = EODPrices.loc[EODPrices.date>EODPrices.transfer_start_date,].reset_index(drop=True)
    EODPrices = EODPrices.drop_duplicates(subset=['code','date'],keep='last').reset_index(drop=True)
    return EODPrices


def listdir(path):
    """
    获取路径下所有文件名
    会忽略隐藏文件

    Output:
        - files: 路径下忽略了隐藏文件所有文件名
    """
    files = os.listdir(path)
    try:files.remove('.DS_store')
    except:pass
    files.sort()
    return files


def LoadMinbar(path):
    """
    用于读取和简单清理交易日的minbar汇总
    """
    minbar_day = pd.read_parquet(path)
    minbar_day['date'] = minbar_day.date.astype(str)
    minbar_day = minbar_day.loc[(minbar_day.min_time<=1455)&(minbar_day.min_time>=930),].reset_index(drop=True)
    minbar_day = minbar_day.loc[(minbar_day.min_time<1130)|(minbar_day.min_time>=1300),].reset_index(drop=True)
    minbar_day = minbar_day.drop_duplicates(['min_time','code'],keep='last')
    return minbar_day

def LoadAShareEODPrices(DataHousepath):
    """
    获取所有正股每个交易日的OHLC等量价数据

    Input:
        - DataHousepath: 日频汇总存储路径
    Output:
        - AShareEODPrices: 日频汇总
    """
    AShareEODPrices = pd.read_parquet(DataHousepath+'AShareEODPrices.parquet')
    AShareEODPrices = AShareEODPrices.rename(columns={'s_info_windcode':'code','trade_dt':'date'})
    AShareEODPrices['date'] = AShareEODPrices['date'].astype(str)
    #过滤掉交易时无法选择转股的可转债
    AShareEODPrices = AShareEODPrices.drop_duplicates(subset=['code','date'],keep='last').reset_index(drop=True)
    AShareEODPrices = AShareEODPrices.drop(['object_id'],axis=1).rename(columns={
        's_dq_preclose':"preclose", 
        's_dq_open':"open", 
        's_dq_high':"high",
        's_dq_low':"low", 
        's_dq_close':"close", 
        's_dq_change':"change", 
        's_dq_pctchange':"pctchange",
        's_dq_volume':"volume", 
        's_dq_amount':"amount", 
        's_dq_adjpreclose':"adjpreclose", 
        's_dq_adjopen':"adjopen",
        's_dq_adjhigh':"adjhigh", 
        's_dq_adjlow':"adjlow", 
        's_dq_adjclose':"adjclose", 
        's_dq_adjfactor':"adjfactor",
        's_dq_avgprice':"avgprice", 
        's_dq_tradestatus':"tradestatus", 
        's_dq_tradestatuscode':"tradestatuscode",
        's_dq_limit':"limit", 
        's_dq_stopping':"stopping", 
        's_dq_adjclose_backward':"adjclose_backward"
    })
    AShareEODPrices = AShareEODPrices.sort_values(by=['date','code'])
    return AShareEODPrices

def GenLastKDay(AShareEODPrices,*args):
    """
    生成包含交易日以及过去K交易日的表单 方便merge

    Input:
        - AShareEODPrices: 日频量价总表单 用于生成交易日历
        - args: 要生成的过去交易日的list
    Output:
        - TradingDate: 交易日历
    """
    TradingDate = pd.DataFrame(AShareEODPrices.date.unique().tolist(),columns=['date']).sort_values(by=['date'])
    for k in args:
        TradingDate['last'+str(k)+'day'] = TradingDate.date.shift(k).reset_index(drop=True)
    return TradingDate.dropna(axis=0).reset_index(drop=True)