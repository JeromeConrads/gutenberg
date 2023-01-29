import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd

count_location = "data/counts/"
count_names = np.array(listdir(count_location))
longest_length = 0
PG = "str"
for i in range(0,count_names.shape[0]):
    location = count_location +count_names[i]
    count_name = str(Path(location))
    print(location)
    txt = pd.read_csv(location, sep="\t", header=None,encoding = "utf-8")
    print(len(txt[0][19]))
    length = len(txt[0][19])
    if length > longest_length:
        longest_length= length
        PG  = count_names[i]

print(PG)