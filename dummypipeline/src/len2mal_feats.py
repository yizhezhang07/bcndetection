import pandas as pd

def compute_length2mal(fqdn, cntMalFQDNs, cntMalDom, histmal):
    if histmal.shape[0] > 0 and (fqdn in histmal["host"].values.tolist()):
        return 0
    if cntMalFQDNs > 0:
        return 2
    elif cntMalDom > 0:
        return 4
    return 10


def gen_len2mal_score(logday, histmalfpath, datafpath, savefpath):
    
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
    
    domscore_fpath = os.path.join(feature_dir, "domscore_raw.csv")
    ipscore_fpath = os.path.join(feature_dir, "ipscore_raw.csv")
    
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