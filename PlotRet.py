# plotting functions
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import numpy as np

def plotNetValue(df,Xname,Yname):
    print('SharpeRatio:',CalSR(df[Yname]))
    plt.figure(figsize=(16,4))
    XaxisNum = df.shape[0]//20
    plt.gca().xaxis.set_major_locator(ticker.MultipleLocator(XaxisNum))
    plt.xticks(rotation=30)
    plt.plot(df[Xname],df[Yname])
    plt.show()

def CalSR(series):
    Er = (series.mean()-1)
    Estd = (series.std()) * (252**0.5) 
    SR = (Er/Estd)
    return float(str(SR)[:5])