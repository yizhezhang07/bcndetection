import numpy as np
import pandas as pd
import scipy
import multiprocessing


def upnsca_method(sig, threshold = 0.6059):
    """
    implementation of periodicity detection algorithm in ''Uncovering Periodic Network Signals of Cyber Attacks''
    We use the original threshold = 0.6059 in the paper
    
    Returns: 
    array: list of detected periods (empty if the method does not report specific detected periods)
    bool: True if the signal is periodic else False
    """
    # since the input signal is always a real number list in our network environment setup
    # we use rfft(https://docs.scipy.org/doc/scipy/reference/generated/scipy.fft.rfft.html) to speed up the computation
    fftSpec = scipy.fft.rfft(sig)
    energyDist = sorted(abs(fftSpec), reverse=True)
    domFreqs = int(len(sig) / 10)
    per = sum(energyDist[:domFreqs]) / sum(energyDist)  
    return [], per > threshold


def upnsca_wrap(df):
    """
    wrap for data frame processing
    """    
    df['periods'], df['detected'] = zip(*df["tdf"].apply(upnsca_method))
    return df


def mltproc_upnsca_wrap(df, maxproc = 16):
    """
    wrap func for multiprocessing
    """    
    pool = multiprocessing.Pool(processes = maxproc)    
    df_lst = np.array_split(df, maxproc)
    res = pool.map(upnsca_wrap, df_lst)
    resdf = pd.concat(res)
    return resdf