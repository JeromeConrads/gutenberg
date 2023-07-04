import pandas as pd
import numpy as np
from os import listdir, path
import dask.dataframe as dd
import glob
from pathlib import Path


#this file loads all the counts files (60.000 Files)
# combines them into 1 File and sums up the total amount each unique Word appears.
# Dask is significantly faster then pandas for this but still takes a while.

# input: data_loc,metadata,name for all_counts
# files: all PGs_counts.txt in metadata,
# output: nothing, creates all_counts
def gen_all_counts(metadata,data_loc = "" , all_counts_name = "all_counts.txt"):
    count_paths = [path.join(data_loc,"data/counts",str(x+"_counts.txt")) for x in metadata.id]

    ddf = dd.read_csv(count_paths, sep="\t", header=None,usecols =[0,1],
                  names=["key", "value"],dtype ={"value": "int64"})
    print(ddf)
    ddf = ddf.dropna(subset=['key'])
    print("na dropped")
    ddf = ddf.groupby(["key"]).agg(total_counts =("value", np.sum),
			       document_frequency = ("key", "count"))
    print(ddf)
    print("start computing")

    df = ddf.compute()
    print("start sort")
    df = df.sort_values("total_counts",ascending=False)
    print("save")
    df.to_csv(path.join(data_loc,"metadata",all_counts_name), sep="\t")
    print("saved and completed")
    
# calls function twice for train and test files
loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"
md = pd.read_csv(path.join(loc,"metadata/test_metadata.csv"))
gen_all_counts(metadata= md,data_loc = loc , all_counts_name = "test_all_counts.txt")

loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"
md = pd.read_csv(path.join(loc,"metadata/train_metadata.csv"))
gen_all_counts(metadata= md,data_loc = loc , all_counts_name = "train_all_counts.txt")
