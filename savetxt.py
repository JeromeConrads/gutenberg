import pandas as pd
import numpy as np
from os import listdir, path
import dask.dataframe as dd
import glob
from pathlib import Path


#
ddf = dd.read_csv(str(Path("D:/Pycharm/gutenberg/data/total_counts/part_counts.*.txt")), sep="\t", header=None,
                  names=["key", "value"])
print(ddf)
ddf = ddf.dropna(subset=['key'])
ddf = ddf.groupby(["key"]).sum()
print(ddf)

df = ddf.compute()
df = df.sort_values("value")
df.to_csv("data/all_counts.txt", header=False, sep="\t")
