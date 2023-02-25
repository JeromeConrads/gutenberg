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
ddf = ddf.groupby(["key"]).agg(total_counts =("value", np.sum),
			       document_frequency = ("key", "count"))
print(ddf)
print("start computing")

df = ddf.compute()
print("start sort")
df = df.sort_values("total_counts",ascending=False)
print("save")
df.to_csv("changed_data/all_counts.txt", sep="\t")
print("saved and completed")
