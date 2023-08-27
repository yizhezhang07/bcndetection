import numpy as np
from scipy import signal, stats


### compute periodogram of the signal
def compute_psd(data, sample_freq=1):
    freq, pxx_den = signal.periodogram(data, sample_freq)
    return freq, pxx_den

### filter the freq using psd_threshold
### then convert freq to periods
def get_potential_periods(freq, psd, psd_threshold):
    freq = np.array(freq)
    psd = np.array(psd)
    res = 1 / freq[psd>psd_threshold]
    return res[res<720]

### get time intervals between connections
def get_ts_intervals(data):
    data_idx = np.arange(len(data))
    data = np.array(data)
    ts_intervals = np.diff(data_idx[data>0])
    return ts_intervals

### get the minimum time intervals
def get_min_tsinterval(ts_intervals):
    if len(ts_intervals) > 0:
        return min(ts_intervals)
    return 0

### filter potential periods
def high_freq_pruning(potential_periods, min_tsintveral):
    return potential_periods[potential_periods>=min_tsintveral]

### ACF of orig signal
def autocorr(x):
    result = np.correlate(x, x, mode='full')
    result = result[result.size//2:]
    result[0] = 0
    return result

### get the peaks of autocorr
def get_autocorr_peaks(data):
    autocor_ts = autocorr(data)
    autocor_ts_norm = autocor_ts / max(autocor_ts)
    peaks, _ = signal.find_peaks(autocor_ts_norm, prominence=0.2)
    return peaks


### filter potential periods using autocorr peaks
### if autocorr peaks - 1 < potential_period < autocorr peaks + 1
def acf_filtered_periodicity(potential_periods, autocorr_peaks, threshold=1):
    true_period = []

    if len(autocorr_peaks) == 0 or len(potential_periods) == 0:
        return true_period
    
    for period in potential_periods:
        if min(abs(autocorr_peaks - period)) <= threshold:
            true_period.append(period)
    return true_period
