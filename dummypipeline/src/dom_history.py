import math
import datetime

import pandas as pd
import numpy as np
import ipaddress
import multiprocessing


def check_age(row, date, aging_out_cond=datetime.timedelta(days=30)):
    """if (log date - last seen date) < 30 days"""
    row_lastseen_dt = datetime.datetime.strptime(row["lastseen_date"], "%Y-%m-%d")
    row_date_dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return row_date_dt - row_lastseen_dt < aging_out_cond

def compute_days_since(date, current_date):
    """ compute delta between two dates"""
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    current_date = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    return (current_date - date).days


def isIP(dom_text):
    """check if string is an ip address"""
    try:
        _ = ipaddress.ip_address(dom_text)
        return 1
    except:
        return 0

    
def gen_periodicity_history(hist_df, zeek_df, compute_info, logtyp="HTTP"):
    
    if logtyp == "HTTP":
        fqdncol = "host"
    elif logtyp == "SSL":
        fqdncol = "server_name"
    else:
        raise ValueError
        
    start_dt = compute_info.get("start_dt")
    start_date = start_dt.strftime("%Y-%m-%d")

    zeek_df["temp"] = 1
    
    df = pd.merge(hist_df, zeek_df, on=[fqdncol], how="outer")
    df.loc[df["count_since_firstseen"].isnull(), "count_since_firstseen"] = 0
    df.loc[df["firstseen_date"].isnull(), "firstseen_date"] = start_date
    df.loc[df["lastseen_date"].isnull(), "lastseen_date"] = start_date
    df.loc[(df["temp"].notnull()) & (df["lastseen_date"].notnull()), "lastseen_date"] = start_date
    

    df["drop"] = df.apply(check_age, axis=1, args=([start_date]))
    df = df.loc[df["drop"]]
    
    df.loc[df[fqdncol].isin(zeek_df[fqdncol]), "count_since_firstseen"] += 1

    df = df.drop(labels=["temp", "drop"], axis=1)

    df["days_since_firstseen"] = df["firstseen_date"].apply(compute_days_since, args=([start_date]))
    df["days_since_lastseen"] = df["lastseen_date"].apply(compute_days_since, args=([start_date]))

    #df.loc[df["isIP"].isnull(), "isIP"] = df.loc[df["isIP"].isnull()][fqdncol].apply(lambda x: isIP(x))
    
    return df


def gen_domain_history(hist_df, zeek_df, compute_info, logtyp="HTTPSSL"):
    
    """
    hist_df: historical data
    zeek_df: daily zeek logs
    compute_info: {"start_dt": log date"}
    logtyp: "HTTPSSL" for aggregated traffic
    
    """
    
    if logtyp == "HTTP":
        fqdncol = "host"
    elif logtyp == "SSL":
        fqdncol = "server_name"
    elif logtyp == "HTTPSSL":
        fqdncol = "host"
    else:
        raise ValueError
        
    start_dt = compute_info.get("start_dt")
    start_date = start_dt.strftime("%Y-%m-%d")

    zeek_df["temp"] = 1
    
    df = pd.merge(hist_df, zeek_df, on=[fqdncol], how="outer")
    df.loc[df["count_since_firstseen"].isnull(), "count_since_firstseen"] = 0
    df.loc[df["firstseen_date"].isnull(), "firstseen_date"] = start_date
    df.loc[df["lastseen_date"].isnull(), "lastseen_date"] = start_date
    df.loc[(df["temp"].notnull()) & (df["lastseen_date"].notnull()), "lastseen_date"] = start_date
    
    df["firstseen_log_type"] = logtyp
    df["lastseen_log_type"] = logtyp

    df["drop"] = df.apply(check_age, axis=1, args=([start_date]))
    df = df.loc[df["drop"]]
    
    df.loc[df[fqdncol].isin(zeek_df[fqdncol]), "count_since_firstseen"] += 1

    df = df.drop(labels=["temp", "drop"], axis=1)

    df["days_since_firstseen"] = df["firstseen_date"].apply(compute_days_since, args=([start_date]))
    df["days_since_lastseen"] = df["lastseen_date"].apply(compute_days_since, args=([start_date]))

    df.loc[df["isIP"].isnull(), "isIP"] = df.loc[df["isIP"].isnull()][fqdncol].apply(lambda x: isIP(x))
    
    return df




    