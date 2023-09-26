import os
import pandas as pd
import numpy as np
   
def merge_cisco(df, cisco, fqdn_col="host"):
    cisco["rank"] = cisco["rank"].apply(lambda x: int(x))
    cisco["cisco_score"] = (1000001-cisco["rank"])/1000000
    tmp = df.merge(cisco[[fqdn_col,"cisco_score"]], on=fqdn_col, how="left")
    tmp[["cisco_score"]] = tmp[["cisco_score"]].fillna(0)
    return tmp


def compute_cisco_score(df, fqdn_col="host"):
    df = df[[fqdn_col, "true_periods", "cisco_score"]]
    df["periodicities"] = df["true_periods"]
    df["integer_pers"] = df["periodicities"].apply(lambda x: np.unique([np.rint(i) for i in x]))

    df_exp = df[["host", "integer_pers", "cisco_score"]].explode("integer_pers")

    per_table = pd.DataFrame(data = df_exp.groupby("integer_pers")[fqdn_col].apply(list))
    per_table["cnt_fqdn_per_periodicity"] = per_table[fqdn_col].map(len)

    per_table_ex = per_table[[fqdn_col, "cnt_fqdn_per_periodicity"]].explode(fqdn_col)
    per_table_ex = per_table_ex.reset_index(drop=False)
    
    res = per_table_ex.groupby(fqdn_col).agg(mean_fqdn_period=('cnt_fqdn_per_periodicity', 'mean'), 
                                             max_fqdn_period=('cnt_fqdn_per_periodicity', 'max'), 
                                             min_fqdn_period=('cnt_fqdn_per_periodicity', 'min'), 
                                             std_fqdn_period=('cnt_fqdn_per_periodicity', 'std'),
                                             min_per=('integer_pers','min'), max_per=('integer_pers', 'max'), 
                                             std_per=('integer_pers', 'std'), mean_per=('integer_pers', "mean")).fillna(0)
    
    res = res.reset_index(drop=False)

    per_score2 = pd.DataFrame(data = df_exp.groupby("integer_pers")["cisco_score"].apply(list))
    per_table_ex = per_table_ex.merge(per_score2, on="integer_pers", how="left")
    per_table_sum2 = pd.DataFrame(data = per_table_ex.groupby(fqdn_col)["cisco_score"].apply(sum))
    per_table_sum2["cisco_min_period"] = per_table_sum2["cisco_score"].apply(min)
    per_table_sum2["cisco_max_period"] = per_table_sum2["cisco_score"].apply(max)
    per_table_sum2["cisco_mean_period"] = per_table_sum2["cisco_score"].apply(np.mean).fillna(0)
    per_table_sum2["cisco_median_period"] = per_table_sum2["cisco_score"].apply(np.median).fillna(0)

    per_table_sum2["cisco_ratio_period"] = per_table_sum2["cisco_score"].apply(lambda x: len([i for i in x if i > 0])/len(x))
    per_table_sum2 = per_table_sum2.reset_index(drop=False)
    
    res = res.merge(per_table_sum2, on=fqdn_col, how="left")
    res = res.drop(columns=["cisco_score"])
    return res


def gen_popularity_score(logday, perfpath, popularityfpath, ciscofpath, savefpath, fqdn_col="host"):
    print("[Info]====== Generate Popularity Features for date:", logday, "======")
    
    if not os.path.exists(perfpath):
        print("[Error] Raw Data File {} not exists.".format(perfpath))
        return -1
    
    perdf = pd.read_parquet(perfpath)
    perdf = perdf[["host", "true_periods"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)
    
    if not os.path.exists(popularityfpath):
        print("[Error] Raw Data File {} not exists.".format(popularityfpath))
        return -1
    
    popularitydf = pd.read_parquet(popularityfpath)
    
    if not os.path.exists(ciscofpath):
        print("[Error] No cisco df")
        return -1
    cisco = pd.read_csv(ciscofpath, names=["rank", fqdn_col])
    
    tmpdf = merge_cisco(perdf, cisco, fqdn_col=fqdn_col)
    
    per_stats = compute_cisco_score(tmpdf)
    per_stats = per_stats.merge(tmpdf[[fqdn_col, "cisco_score"]], on=fqdn_col, how="left")
    per_stats = per_stats.merge(popularitydf[[fqdn_col, "fqdn_popularity"]], on=fqdn_col, how="left").fillna(0)
    
    print("[Info] Features Shape", per_stats.shape)
    per_stats.to_parquet(savefpath)
    print("[Info] Popularity Features Saved to:", savefpath)
    return per_stats



def compute_histmal_score(df, fqdn_col="host"):
    df = df[[fqdn_col, "true_periods", "total_maleng", "label"]]
    df["periodicities"] = df["true_periods"]
    df["integer_pers"] = df["periodicities"].apply(lambda x: np.unique([np.rint(i) for i in x]))

    df_exp = df[["host", "integer_pers", "label", "total_maleng"]].explode("integer_pers")

    per_table = pd.DataFrame(data = df_exp.groupby("integer_pers")[fqdn_col].apply(list))
    per_table["cnt_fqdn"] = per_table[fqdn_col].map(len)

    per_table_ex = per_table[[fqdn_col, "cnt_fqdn"]].explode(fqdn_col)
    per_table_ex = per_table_ex.reset_index(drop=False)

    per_score = pd.DataFrame(data = df_exp.groupby("integer_pers")["label"].apply(list))

    per_table_ex = per_table_ex.merge(per_score, on="integer_pers", how="left")
    per_table_sum = pd.DataFrame(data = per_table_ex.groupby(fqdn_col)["label"].apply(sum))
    per_table_sum["hist_malscore_min_period"] = per_table_sum["label"].apply(min)
    per_table_sum["hist_malscore_max_period"] = per_table_sum["label"].apply(max)
    per_table_sum["hist_malscore_mean_period"] = per_table_sum["label"].apply(np.mean).fillna(0)
    per_table_sum["hist_malscore_median_period"] = per_table_sum["label"].apply(np.median).fillna(0)

    per_table_sum["hist_malscore_ratio_period"] = per_table_sum["label"].apply(lambda x: len([i for i in x if i > 0])/len(x))
    per_table_sum = per_table_sum.reset_index(drop=False)
    per_table_sum = per_table_sum.drop(columns=[ "label"])
    
    return per_table_sum


def gen_hist_malscore(logday, perfpath, savefpath,
                      histfpath="dummydata/malicious_hist.csv"):

    print("[Info]====== Generate hist Mal Features for date:", logday, "======")
   
    if not os.path.exists(perfpath):
        print("[Error] Raw Data File {} not exists.".format(perfpath))
        return -1
    
    if not os.path.exists(histfpath):
        print("[Error] Raw Feature File {} not exists.".format(histfpath))
        return -1

    perdf = pd.read_parquet(perfpath)
    histdf = pd.read_csv(histfpath)
    perdf = perdf[["host", "true_periods"]].merge(histdf, on="host", how="left")
    perdf = perdf[["host", "true_periods",  "label", "total_maleng"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)

    per_stats = compute_histmal_score(perdf).fillna(0)
    
    print("[Info] Features Shape", per_stats.shape)
    per_stats.to_parquet(savefpath)
    print("[Info] hist Mal Features Save Data to:", savefpath)
    
    return per_stats