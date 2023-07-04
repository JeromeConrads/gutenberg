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

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"



sys.path.append(os.path.join(data_loc,'src'))
from metaquery import meta_query
mq = meta_query(path=os.path.join(data_loc,'metadata','clean_metadata.csv'))
print(mq.df)
three_most_common_sub = mq.get_subjects_counts().most_common(3)
df_3_common = pd.DataFrame()
for sub in three_most_common_sub:
    print(sub)
    df_3_common = pd.concat([df_3_common,mq.df[mq.df["subjects"].str.contains(str("'"+ sub[0]+"'")).replace(np.nan,False)]])
    
df_3_common = df_3_common.drop_duplicates().drop(columns=['Unnamed: 0'])
print(df_3_common.columns)

pa =path.join(data_loc,"smalldata")
if not os.path.exists(pa):
      
    # if the demo_folder directory is not present 
    # then create it.
    os.makedirs(path.join(pa,"metadata"))
    os.makedirs(path.join(pa,"data/counts"))
    os.makedirs(path.join(pa,"data/text"))    
    os.makedirs(path.join(pa,"data/tokens"))  
df_3_common.to_csv(path.join(pa,"metadata/metadata.csv"))

# copy data files into new folder
for PG in df_3_common.id:
#    old_loc = path.join(data_loc,"data/counts",str(PG+"_counts.txt"))
#    new_loc = path.join(data_loc,"smalldata/data/counts")
#    shutil.copy(old_loc,new_loc)

#    old_loc = path.join(data_loc,"data/text",str(PG+"_text.txt"))
#    new_loc = path.join(data_loc,"smalldata/data/text")
#    shutil.copy(old_loc,new_loc)

    old_loc = path.join(data_loc,"data/tokens",str(PG+"_tokens.txt"))
    new_loc = path.join(data_loc,"smalldata/data/tokens")
    shutil.copy(old_loc,new_loc)
    
    print(str(PG) + " copyied")



    
    
    

