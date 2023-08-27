import numpy as np
import pandas as pd
import emd
import multiprocessing

from .helpfns import *



### random permute data and find PSD threshold as the (permute_cnt * Confidence)-th Maximum PSD value
def bcn_permute(data, permute_cnt=100, C=0.95, sample_freq=1):
    max_psd = []
    for i in range(permute_cnt):
        permuted_data = np.random.permutation(data)
        _, t_psd = compute_psd(permuted_data, sample_freq)
        max_power = np.max(t_psd)
        max_psd.append(max_power)
        
    rank = int(C * permute_cnt) - 1
    max_psd.sort()
    psd_threshold = max_psd[rank]
    return psd_threshold


def emd_compose(signals):
    imf_opts = {'sd_thresh': 0.05}
    try: 
        emdsig = emd.sift.sift(signals, imf_opts=imf_opts, max_imfs=2)[:,0]
        return emdsig
    except:
        return signals


### filter potential periods using autocorr peaks
### Larger threshold here due to the fact that emd decomposition will shif the signals
def bcn_filtering(potential_periods, autocorr_peaks, threshold=2):
    true_period = []
    if len(autocorr_peaks) == 0 or len(potential_periods) == 0:
        return true_period
    
    for period in potential_periods:
        if min(abs(autocorr_peaks - period)) <= threshold:
            true_period.append(period)
    return true_period
    
def bcndetection_method(signals):
    """
    implementation of our periodicity detection algorithm
    
    Returns: 
    array: list of detected periods (empty if the method does not report specific detected periods)
    bool: True if the signal is periodic else False
    """
    periods = []
    detected = False
    
    sigcnt = len(signals[signals>0])
    if sigcnt < 3:
        return periods, detected
    
    # decompose
    signals = emd_compose(signals)
    freq, psd = compute_psd(signals)
    psd_threshold = bcn_permute(signals)
    potential_pers = get_potential_periods(freq, psd, psd_threshold)
    
    # if no valid periodicity
    if len(potential_pers) == 0:
        return periods, detected

    ts_intervals = get_ts_intervals(signals)
    min_ts = get_min_tsinterval(ts_intervals) 
    high_freq_periods = high_freq_pruning(potential_pers, min_ts)
    
    # if no valid periodicity
    if len(high_freq_periods) == 0:
        return periods, detected
    
    ## acf verification
    acf_peaks = get_autocorr_peaks(signals)
    periods = bcn_filtering(high_freq_periods, acf_peaks)
    if len(periods) > 0:
        detected = True
        
    return periods, detected

    
def bcndetection_wrap(df):
    """
    wrap for data frame processing
    """
    df['periods'], df['detected'] = zip(*df["tdf"].apply(bcndetection_method))
    return df

def mltproc_bcndetection_wrap(df, maxproc = 16):
    """
    wrap func for multiprocessing
    """    
    pool = multiprocessing.Pool(processes = maxproc)    
    df_lst = np.array_split(df, maxproc)
    res = pool.map(bcndetection_wrap, df_lst)
    resdf = pd.concat(res)
    return resdf

