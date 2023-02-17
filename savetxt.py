import pandas as pd
import numpy as np
from os import listdir, path
import dask.dataframe as dd
import glob
from pathlib import Path

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

#this file loads all the counts (60.000 Files)
# combines them into 1 File and sums up the total amount each unique Word appears.
# Dask is significantly faster then pandas for this but still takes a while.
count_path = path.join(data_loc,"data/counts/PG*_counts.txt")

ddf = dd.read_csv(count_path, sep="\t", header=None,
                  names=["key", "value"])
print(ddf)
ddf = ddf.dropna(subset=['key'])
print("na dropped")
ddf = ddf.groupby(["key"]).sum()
print(ddf)
print("summed up")

df = ddf.compute()
print("computed into pandas")
df = df.sort_values("value")
print("sorted")
df.to_csv("changed_data/all_counts.txt", header=False, sep="\t")
print("saved and completed")
