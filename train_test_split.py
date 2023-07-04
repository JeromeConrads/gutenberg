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
mq = meta_query(path= path.join(data_loc,'metadata','metadata.csv'),filter_exist=False)
three_most_common_sub = mq.get_subjects_counts().most_common(3)
#create test and train sets
# with 3 most common subjects
# ONLY include books that contain exactly 1 of the subjects
#so its not a multi topic problem
#later will (hopefully) use bookshelves

# remove all entries containing more then 1 of the subjects

print([str(x[0]) for x in three_most_common_sub])
lst = ["'Science fiction'", "'Short stories'", "'Fiction'"]
print(lst)
S = set(lst)
out = mq.df[[len(S.intersection(x.split(', ')))<2 for x in mq.df["subjects"].str.strip('{}')]]
print(out)
# set random state for reproducability
train,test = train_test_split(out, test_size=0.25, random_state=42)
print(train)
print(test)
test.to_csv(path.join(data_loc,"metadata/test_metadata.csv"), sep=",",index = False)
train.to_csv(path.join(data_loc,"metadata/train_metadata.csv"), sep=",",index = False)

