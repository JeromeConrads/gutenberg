import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd
import csv,sys
import re
import shutil
from sklearn.model_selection import train_test_split

# split the metadata and counts into train and test set
# creates train_metadata.csv and test_metadata.csv
# creates train_data folder and test_data folder

# Right now only creates the split for the most common Tags (3 most common)
# later may try to make sets for all tags
# for that we need to know what tag on a book is the more common one.

# Books that have multiple tags may appear multiple times

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata/"

sys.path.append(path.join(data_loc,'src'))
from metaquery import meta_query
path1 = path.join(data_loc,'metadata','clean_metadata.csv')
mq = meta_query(path= path1,filter_exist=False)
print(mq.df)
print(mq.df.columns)
print(path1)

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

sys.path.append(path.join(data_loc,'src'))
path2 = path.join(data_loc,'metadata','clean_metadata.csv')
mq = meta_query(path= path2,filter_exist=False )
print(mq.df)
print(mq.df.columns)
print(path2)
