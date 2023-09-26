import pandas as pd
import datetime
import os


### compute local history data
def compute_domhist(perdf, firstseen_col="firstseen_date", lastseen_col="lastseen_date", drange=30):
    df = perdf.copy()
    df["fst_date"] = df[firstseen_col].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d") if x is not None else None)
    df["lst_date"] = df[lastseen_col].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d")if x is not None else None)
    df["occ_date"] = df.apply(lambda x: x["lst_date"] - x["fst_date"] if x[firstseen_col] is not None else None, axis=1)
    df["occ_days"] = df["occ_date"].apply(lambda x: x.days) + 1
    df["occ_days"] = df["occ_days"].fillna(0)
    df["freq"] = df["count_since_firstseen"] / drange    
    return df[["host", "occ_days", "freq"]]


def compute_hist_features(perdf, histdf, logday):
    histdf = histdf.loc[histdf["lastseen_date"] == logday]
    res = compute_domhist(histdf, "firstseen_date", "lastseen_date")
    res["occ"] = 1/res["occ_days"]
    res = res.drop(columns=["occ_days"])
    perdf = perdf[["host"]].merge(res, on="host", how="left")
    perdf = perdf.fillna(0)
    return perdf 


def gen_history_score(logday, datafpath, histfpath, savefpath):
    ### read data
    if not os.path.exists(datafpath):
        print("[Error] Raw Data File {} not exists.".format(datafpath))
        return -1
    
    ### Read history df
    if not os.path.exists(histfpath):
        print("[Error] Raw Data File {} not exists.".format(histfpath))
        return -1
    
    perdf = pd.read_parquet(datafpath)
    perdf = perdf[["host"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)

    hist = pd.read_parquet(histfpath)
    print("[Info] History Data shape:", hist.shape)
 
    ### Compute
    fqdn_col = "host"
    res = compute_hist_features(perdf, hist, logday)   
    print("[Info] Features Shape", res.shape)
    
    res.to_parquet(savefpath)
    print("[Info] History Features Saved to:", savefpath)
    return res

