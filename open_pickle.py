import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd
import csv,sys,os
import re
import shutil
# choose a small amount of tags around5-20
# make a new metadata list only containing them
# make a new folder only containing the PGS. 
# train test split them for each tag
# calculate keyness
# build model
# ???
# Profit

import pandas as pd

# create a sample dataframe
df = pd.DataFrame({'Column1': ["{'string1'}", 'string2', 'string3']})

# create a sample array of substrings
substrings = ["'g1'", 'g2',"g3"]

# loop through each substring and replace the corresponding values in the dataframe
for i, substring in enumerate(substrings):
    df['Column1'] = df['Column1'].apply(lambda x: str(i) if substring in x else x)

print(df)
    
    

