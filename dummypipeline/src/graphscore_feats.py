import pandas as pd
import tldextract
import ipaddress
import os


def compute_ipscore(df, fqdn_col="host"):
    res = df.groupby(fqdn_col).agg(sum_ipDom=('cntDom', 'sum'), sum_ipMalDom=('cntMalDom', 'sum'),
                                   avg_ipDom=('cntDom', 'mean'), avg_ipMalDom=('cntMalDom', 'mean'),
                                   max_ipDom=('cntDom', 'max'), max_ipMalDom=('cntMalDom', 'max'),
                                   min_ipDom=('cntDom', 'min'), min_ipMalDom=('cntMalDom', 'min'),
                                   max_ipMalDomRatio=('malIPDom_ratio', 'max'), max_ipDomMalEngRatio=('malIPDomEng_ratio', 'max'),
                                   min_ipMalDomRatio=('malIPDom_ratio', 'min'), min_ipDomMalEngRatio=('malIPDomEng_ratio', 'min'), 
                                   avg_ipMalDomRatio=('malIPDom_ratio', 'mean'), avg_ipDomMalEngRatio=('malIPDomEng_ratio', 'mean'),
                                   sum_ipDomMalEng=('sumIPDomMalEng', 'sum'), max_ipDomMalEng=('maxIPDomMalEng', 'max'), 
                                   min_ipDomMalEng=('minIPDomMalEng', 'min'), avg_ipDomMalEng=('avgIPDomMalEng', 'mean')).fillna(0)
    
    res = res.reset_index(drop=False)
    return res


def gen_dom_graphscore(logday, datafpath, savefpath, rawfpath):
    
    print("[Info]====== Generate Domain Graph Features for date:", logday, "======")
  
    if not os.path.exists(datafpath):
        print("[Error] Raw Data File {} not exists.".format(datafpath))
        return -1
    
    if not os.path.exists(rawfpath):
        print("[Error] Raw Feature File {} not exists.".format(rawfpath))
        return -1
        
    perdf = pd.read_csv(datafpath)[["host","domain"]]
    perdf = perdf[["host", "domain"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)

    dom_scoredf = pd.read_csv(rawfpath)
    dom_scoredf = dom_scoredf.fillna(0).drop_duplicates()
    dom_scoredf["malFQDN_ratio"] = dom_scoredf["cntMalFQDNs"]/dom_scoredf["cntFQDN"]
    dom_scoredf["malEng_ratio"] = dom_scoredf["sumMalEng"] / dom_scoredf["cntFQDN"]

    perdf = perdf[["host", "domain"]].merge(dom_scoredf, on="domain", how="left")
    print(perdf.shape)
    
    perdf.to_parquet(savefpath)
    print("[Info] Graph Domain Features Save Data to:", savefpath)
    return perdf

def gen_ip_graphscore(logday, datafpath, savefpath, rawfpath):
    
    print("[Info]====== Generate IP Graph Features for date:", logday, "======")
    
    if not os.path.exists(datafpath):
        print("[Error] Raw Data File {} not exists.".format(datafpath))
        return -1
    
    if not os.path.exists(rawfpath):
        print("[Error] Raw Feature File {} not exists.".format(rawfpath))
        return -1
    

    perdf = pd.read_csv(datafpath)
    perdf = perdf[["host", "id_resp_h"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)

    ipscoredf = pd.read_csv(rawfpath)
    ipscoredf = ipscoredf.rename(columns={"ip":"id_resp_h"})
    ipscoredf = ipscoredf.fillna(0).drop_duplicates()
    
    ipscoredf["malIPDom_ratio"] = ipscoredf["cntMalDom"]/ipscoredf["cntDom"]
    ipscoredf["malIPDomEng_ratio"] = ipscoredf["sumIPDomMalEng"] / ipscoredf["cntDom"]
    perdf = perdf[["host", "id_resp_h"]].merge(ipscoredf, on="id_resp_h", how="left")
    res = compute_ipscore(perdf)
    print("[Info] IP features:", res.shape)
    
    res.to_parquet(savefpath)
    print("[Info] Graph IP Features Save Data to:", savefpath)
    return res


def compute_length2mal(fqdn, cntMalFQDNs, cntMalDom, histmal):
    if histmal.shape[0] > 0 and (fqdn in histmal["host"].values.tolist()):
        return 0
    if cntMalFQDNs > 0:
        return 2
    elif cntMalDom > 0:
        return 4
    return 10 # set up a large number to distinguish between has malicious neighbor vs not


def gen_len2mal_score(logday, datafpath, savefpath, 
                      histmalfpath, domscore_fpath, ipscore_fpath):
    
    """
    Finding nearest malicious node can be done using cypher queries in neo4j database.
    If you have a small dataset, We recommand setting up a min and a max 
    (e.g. match (n)-[r*1..5]->(m)) during the query.
    However, even with the above optimization,
    it's very slow given the sheer amount of noed in our graph db.
    During deployment phase, our graph db holds ~2 million nodes on a daily basis in the database. 
    
    Therefore, we use an propagation method to approximate the length, explained as below:
    For each domain, we have FQDN1 -> domain, FQDN2 -> domain, ... etc
    the domain malicious score indicates whether there're any malicious FQDN connect to it. 
    When domain.cntMalFQDNs > 0, it suggests that there are malicious FQDNs hosted on the same domain. 
    For example: FQDN (new) -> domain <- FQDN (malicious). 
    Therefore, when domain.cntMalFQDNs > 0, the nearest lenghth to malicious neighbor is 2.
    
    Similarly, for each IP, we have FQDN1-> domain1 -> IP <- domain2 <- FQDN2, etc.
    The IP.cntMalDom suggests whether the neighboring domain has connections to malicious FQDN.
    Therefore, when domain.cntMalFQDNs = 0 and IP.cntMalDom > 0, 
    the nearest lenghth to malicious neighbor is 4.
    """
    
    print("[Info]====== Generate Graph Connection Features for date:", logday, "======")

    if (not os.path.exists(domscore_fpath)) or (not os.path.exists(ipscore_fpath)):
        print("[Error] Missing domscore and ipscore data.")
        return -1

    
    print("[Info] Read historical malicious data:", histmalfpath)
    if os.path.exists(histmalfpath):
        histmal = pd.read_csv(histmalfpath) 
    else:
        print("[Error] Missing historical malicious data.")
        return -1
        

    print("[Info] Read Raw Data from: ", datafpath)

    if not os.path.exists(datafpath):
        print("[Error] Raw Data File {} not exists.".format(datafpath))
        return refdata, -1

    perdf = pd.read_csv(datafpath)
    perdf = perdf[["host", "id_resp_h", "domain"]].drop_duplicates()
    print("[Info] Raw Data shape:", perdf.shape)
    
    
    dom_scoredf = pd.read_csv(domscore_fpath)
    dom_scoredf = dom_scoredf.fillna(0)[["domain", "cntMalFQDNs"]].drop_duplicates()
    perdf = perdf.merge(dom_scoredf, on="domain", how="left")
    
    ipscoredf = pd.read_csv(ipscore_fpath)
    ipscoredf = ipscoredf.rename(columns={"ip":"id_resp_h"})[["id_resp_h", "cntMalDom"]]
    ipscoredf = ipscoredf.fillna(0).drop_duplicates()
    perdf = perdf.merge(ipscoredf, on="id_resp_h", how="left")
    
    perdf["len2malFQDN"] = perdf.apply(lambda x: compute_length2mal(x['host'], x["cntMalFQDNs"], x["cntMalDom"], histmal), axis=1)
    perdf = perdf.groupby("host").agg(minlen2malFQDN = ('len2malFQDN', 'min'), avglen2malFQDN = ('len2malFQDN', 'mean'),
                                      maxlen2malFQDN = ('len2malFQDN', 'max'))
    perdf = perdf.reset_index(drop=False)
    
    print("[Info] Features Shape", perdf.shape)
    perdf.to_parquet(savefpath)
    print("[Info] Graph Connection Features Save Data to:", savefpath)
    return perdf