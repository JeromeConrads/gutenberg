import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd


data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

count_location = path.join(data_loc,"data/counts/")
count_names = np.array(listdir(count_location))
Document_amount = count_names.shape[0]
all_counts_df = pd.read_csv("changed_data/all_counts.txt",usecols = ["key","document_frequency"],index_col = "key", sep="\t")


txt = pd.read_csv(path.join(count_location,count_names[0]),usecols =[0,1], names=["key", "term_freq"], sep="\t")
txt.to_csv(path.join(count_location,count_names[0]), sep="\t",header = None,index = False)
