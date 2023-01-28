import pandas as pd
import numpy as np
from os import listdir, path

count_location = "data/counts/"
file = "PG58312_counts.txt"
file2 = "PG100_counts.txt"
print(path.getsize(count_location + file))
txt = pd.read_csv(count_location + file2, sep="\t", header=None)
txt2 = pd.read_csv(count_location + file2, sep="\t", header=None)
txt2.columns =["0","1"]
txt2 = txt2.astype({"0": str, "1" : np.int})
print(txt.dtypes)
print(txt.memory_usage())
print(txt2.dtypes)
print(txt2.memory_usage())
np.savetxt(r'data/total_counts/np.txt', txt.values, fmt='%s', delimiter="\t")
