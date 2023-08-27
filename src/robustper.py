import numpy as np
import pandas as pd
import scipy
import multiprocessing

from .robustperiod import robust_period_full


def robustper_method(x):
    """"
    wrap the unofficial implementation of RobustPeriod: Time-Frequency Mining for Robust Multiple Periodicities Detection
    https://github.com/ariaghora/robust-period
    
    Warning: extremely slow
    
    Returns: 
    array: list of detected periods (empty if the method does not report specific detected periods)
    bool: True if the signal is periodic else False
    """
    lmb = 1e+6
    c = 2
    num_wavelets = 2
    zeta = 1.345
    
    detected = False
    periods = []
    
    sigcnt = len(x[x>0])
    
    if sigcnt < 3:
        return periods, detected
    
    periods = robust_period_full(x, 'db10', num_wavelets, lmb, c, zeta)[0]
    
    if len(periods) > 0:
        detected = True
    
    return periods, detected
    
def robustper_wrap(df):
    """
    wrap for data frame processing
    """
    df['periods'], df['detected'] = zip(*df["tdf"].apply(robustper_method))
    return df


def mltproc_robustper_wrap(df, maxproc = 16, mute = False):
    """
    wrap func for multiprocessing
    """    
    pool = multiprocessing.Pool(processes = maxproc)

    df_lst = np.array_split(df, maxproc)
    res = pool.map(robustper_wrap, df_lst)

    resdf = pd.concat(res)
    return resdf
