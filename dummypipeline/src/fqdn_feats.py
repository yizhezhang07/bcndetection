### FQDN-based features
import numpy as np
import pandas as pd
import datetime
import ipaddress
import tldextract
import math
import os

# compute if a domain name is an IP address
def isIP(dom_text):
    try:
        _ = ipaddress.ip_address(dom_text)  # Ignore the return type no need to allocate for a variable...
        return 1
    except:
        return 0

# compute the domain name length
def dom_len(dom_text):
    try:
        return len(dom_text)
    except:
        return 0

# parse the tld of the fqdn
def dom_tld(dom_text):
    try:
        return tldextract.extract(dom_text).suffix
    except:
        return ""


# parse the second level domain of the fqdn
def dom_sld(dom_text):
    try:
        return tldextract.extract(dom_text).domain
    except:
        return ""

# parse the subdomain of the fqdn
def dom_sub(dom_text):
    try:
        return tldextract.extract(dom_text).subdomain
    except:
        return ""


# compute the entropy of a string
def entropy_str(dom_text):
    """
    input: domain_text: str
    return: entropy of the string
    """
    try:
        length = len(dom_text)
        prob = [float(list(dom_text).count(c)) / length for c in set(dom_text)]
        return -sum([p * math.log2(p) for p in prob])
    except:
        return 0

# cnt the level of the domain, e.g 'viginia.edu'=>2 'cs.virginia.edu'=>3
def dom_cnt(dom_text):
    if dom_text == "":
        return 0
    return len(dom_text.split("."))


def isMissing(dom_text):
    if dom_text == "missing" or dom_text == "":
        return 1
    return 0


def isIllegal(domisIP, domisMissing, domtld):
    if domisIP or domisMissing or domtld=="":
        return 1
    return 0


def compute_fqdn_features(df, fqdncol="host"):
    df["isIP"] = df[fqdncol].apply(lambda x: isIP(x))
    df["dom_tld"] = df[fqdncol].apply(lambda x: dom_tld(x))
    df["dom_sld"] = df[fqdncol].apply(lambda x: dom_sld(x))
    df["dom_sub"] = df[fqdncol].apply(lambda x: dom_sub(x))
    df["isMissing"] = df[fqdncol].apply(lambda x: isMissing(x))
    df["domain"] = df.apply(lambda x: ".".join([x["dom_sld"], x["dom_tld"]]) if x["isIP"]== 0 else x[fqdncol], axis=1)
    df["dom_illegal"] = df.apply(lambda x: isIllegal(x['isIP'], x['isMissing'], x['dom_tld']), axis=1)
    
    df["dom_sld_entropy"] = df.apply(lambda x: (entropy_str(x['dom_sld']) if x['isIP']==0 and x['isMissing']==0 else 0), axis=1)
    df["subdom_entropy"] = df.apply(lambda x: (entropy_str(x['dom_sub']) if x['isIP']==0 and x['isMissing']==0 else 0), axis=1)
    df["dom_entropy"] = df.apply(lambda x: (entropy_str(x['domain']) if x['isIP']==0 and x['isMissing']==0 else 0), axis=1)
    df["fqdn_entropy"] = df.apply(lambda x: entropy_str(x[fqdncol]), axis=1)
    
    df["dom_tldcnt"] = df.apply(lambda x: (1 if x['dom_tld']=="" else 0), axis=1) # if fqdn has tld, 1, otherwise 0
    df["dom_sldcnt"] = df.apply(lambda x: (1 if x['dom_sld']=="" else 0), axis=1) # if fqdn has sld, 1, otherwise 0
    # split subdomain by '.' to get subdomain level, e.g. api.test = 2
    df["dom_subcnt"] = df.apply(lambda x: dom_cnt(x['dom_sub']) , axis=1)
    df["dom_level"] = df.apply(lambda x: (x['dom_tldcnt'] + x['dom_sldcnt'] + x['dom_subcnt']), axis=1)
    
    df["dom_length"] = df.apply(lambda x: (dom_len(x[fqdncol]) if x['isIP']==0 and x['isMissing']==0 else 0), axis=1)
    
    return df


def gen_fqdn_features(logday, perfpath, savefpath, fqdn_col="host"):
    print("[Info]====== Generate Popularity Features for date:", logday, "======")
    
    if not os.path.exists(perfpath):
        print("[Error] Raw Data File {} not exists.".format(perfpath))
        return -1
    
    perdf = pd.read_parquet(perfpath)
    perdf = perdf[["host", "psd_ratio"]].drop_duplicates(["host"])
    print("[Info] Raw Data shape:", perdf.shape)
    
    res = compute_fqdn_features(perdf)
    res = res[['host', 'psd_ratio', 'dom_illegal', 'dom_sld_entropy', 'subdom_entropy', 'dom_entropy',
       'fqdn_entropy', 'dom_tldcnt', 'dom_sldcnt', 'dom_subcnt', 'dom_level', 'dom_length']]
    print("[Info] Features Shape", res.shape)
    res.to_parquet(savefpath)
    print("[Info] Popularity Features Saved to:", savefpath)
    return res