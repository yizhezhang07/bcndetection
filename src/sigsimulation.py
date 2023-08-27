import numpy as np
import random
import pandas as pd

def omit(rate):
    return random.random() <= rate

def gen_periodic_signal_insec(period, std, omit_rate=0, length=1440 * 60):
    base = np.zeros(length)
    
    std_mult_period = std * period
    
    for i in range(length):
        
        if i % period == 0:
            
            if std == 0:
                shift = 0
            else:
                shift = round(np.random.normal(0, std_mult_period))
            
            if (i + shift) < 0 or (i+shift) >= length:
                continue
            else:
                ## if the signal is not omitted, create signal
                if not omit(omit_rate):
                    base[i + shift] += 1
    return base


def resample_sig(sig, samplerate = 60):
    # resample signal
    return np.array([sum(sig[i: i+samplerate]) for i in range(0, len(sig), samplerate)])


def gen_signal_df(period, std=0, omit_rate=0, count=100, length=1440, samplerate=60):
    sigl = []
    
    for i in range(count):
        sig = np.zeros(length)
        sig_1sec = gen_periodic_signal_insec(period=period, std=std, omit_rate=omit_rate, length=length*60)
        sig += resample_sig(sig_1sec, samplerate)
        sigl.append(sig)
    
    sigdf = pd.DataFrame()
    sigdf["tdf"] = sigl
    sigdf["ts_cnt"] = sigdf["tdf"].apply(lambda x: len(x[x>0]))
    return sigdf


def add_poisson_insert(period, std=0, prate=0., maxarrival=100, lam=5, length=1440 * 60):
    base = np.zeros(length)
        
    for i in range(length):
         if i % period == 0:
            base[i] += 1
            
            if prate > 0:
                width = prate * period
                _arrival_time = 0
                for j in range(maxarrival):
                    _inter_arrival_time = random.expovariate(lam)
                    scaled_inter_arrival = round(width * _inter_arrival_time)
                    _arrival_time = _arrival_time + scaled_inter_arrival
                    if _arrival_time > width:
                        break
                    else:
                        base[i+round(_arrival_time)] += 1

    return base


def gen_poisson_signal_df(period, std=0, omit_rate=0, prate=0, count=100, maxarrival=100, lam=5, length=1440, samplerate=60):
    sigl = []
    
    for i in range(count):
        sig = np.zeros(length)
        sig_1sec = add_poisson_insert(period=period, std=std, prate=prate, maxarrival=maxarrival, lam=lam, length=length*60)
        sig += resample_sig(sig_1sec, samplerate)
        sigl.append(sig)
    
    sigdf = pd.DataFrame()
    sigdf["tdf"] = sigl
    sigdf["ts_cnt"] = sigdf["tdf"].apply(lambda x: len(x[x>0]))
    return sigdf