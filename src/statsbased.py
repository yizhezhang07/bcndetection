import numpy as np
import pandas as pd
import multiprocessing

## import local functions
from .helpfns import *


def stats_method(sig, threshold = 0.007):
    """
    implementation of periodicity detection algorithm in ''A malware beacon of botnet by local periodic communication behavior''
    We use the original threshold = 0.007 in the paper
    Parameters:
        sig (array): input time series
        threshold (float)
    Returns: 
    array: list of detected periods (empty if the method does not report specific detected periods)
    bool: True if the signal is periodic else False
    """
    ts_intervals = get_ts_intervals(sig)
    score = np.std(ts_intervals) / np.mean(ts_intervals)
    return [], score < threshold

def stats_wrap(df):
    """
    wrap func for multi process
    """    
    df['periods'], df['detected'] = zip(*df["tdf"].apply(stats_method))
    return df

def mltproc_stats_wrap(df, maxproc = 16):
    """
    wrap func for multiprocessing
    """    
    pool = multiprocessing.Pool(processes = maxproc)    
    df_lst = np.array_split(df, maxproc)
    res = pool.map(stats_wrap, df_lst)
    resdf = pd.concat(res)
    return resdf