import pandas as pd

def gen_popular_host(zeek_df, fqdn_col="host", client_col="id_orig_h"):
    """
    generate local popularity list based on client IP "id_orig_h"
    """
    # total client on that day
    totalclients = len(zeek_df[client_col].drop_duplicates().values)
    
    # count of distinct client IPs 
    res = zeek_df[[fqdn_col,client_col]].groupby([fqdn_col]).nunique().reset_index()
    
    res["fqdn_popularity"] = res[client_col]/totalclients
    
    return res[[fqdn_col, "fqdn_popularity"]]