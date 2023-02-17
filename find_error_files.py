import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

# this file searches for PG counts that are spezial cases.
# f.e.  files with extremly long words
count_location = path.join(data_loc,"data/counts/")
count_names = np.array(listdir(count_location))
longest_length = 0
PG = []
words = []
for i in range(0,count_names.shape[0]):
    location = count_location +count_names[i]
    count_name = str(Path(location))
    print(location)
    txt = pd.read_csv(location, sep="\t", header=None,encoding = "utf-8")
    print(len(txt[0][19]))
    length = len(txt[0][19])
    if length > 20:
        longest_length= length
        PG  += [count_names[i]]
        words += [txt[0][19]]
for i,j in PG,words:
	print(str(i) +" " + str(j))
