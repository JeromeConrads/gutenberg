from pathlib import Path
from typing import List

import pandas as pd
import numpy as np
from os.path import exists
from os import remove,listdir,path

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

# I remove all entries and that are not completely in english
# successful entries contain just ['en'] in langauge column in metadata.csv
# also remove entrys that are wrong encoded or empty

metadata_path = str(Path("metadata/metadata.csv"))
metadata = pd.read_csv(metadata_path, sep=",")
print(metadata.loc[0])

is_en_bitmap = metadata["language"].str.contains("['en']", regex=False)

non_en_metadata = metadata[~ is_en_bitmap]
print(non_en_metadata.id.iloc[0])

non_en_metadata.to_csv("changed_data/non_en_metadata.txt", sep=",")


# remove from metadata
# raw/PGx_raw.txt
# text/PGx_text.txt
# tokens/PGx_tokens.txt
# counts/PGx_counts.txt

def del_file(path_to_file):
    if exists(path_to_file):
        remove(path_to_file)
        return 1
    else:
        return 0

def remove_data(metadataframe, data_Path):
    file_Paths = ["data/counts/", "data/raw/", "data/text/", "data/tokens/"]
    file_extensions = ["_counts.txt", "_raw.txt", "_text.txt", "_tokens.txt", ]
    successfully_removed = [0, 0, 0, 0]
    for j in range(0, metadataframe.shape[0]):
        for i in range(0, len(file_Paths)):
            full_Path = path.join(data_Path, file_Paths[i], metadataframe.id.iloc[j], file_extensions[i]
            print("removed "+ str(metadataframe.id.iloc[j]))
            if exists(full_Path):
                successfully_removed[i] += del_file(full_Path)
    print(successfully_removed)

remove_data(non_en_metadata,data_loc)
#remove the removed metadata from metadata
clean_metadata = pd.concat([metadata, non_en_metadata, non_en_metadata]).drop_duplicates(keep=False)
clean_metadata.to_csv("changed_data/clean_metadata.csv", sep=",")
print(clean_metadata.shape)
print(metadata.shape)

# removing of error counts.
# counts with no words 
# trying to catch corrupted count files

path_counts = path.join(data_loc,"data/counts/")
count_names = np.array(listdir(path_counts))
removed_counts = [0,0]
removed_names = []
for name in count_names:
    if (path.getsize(path_counts+name) > 10): 
        txt = pd.read_csv(path_counts+name, sep="\t", header=None,encoding = "utf-8")
        if txt.shape[0] < 20:
            del_file(path_counts+name)
            removed_counts[1] = removed_counts[1]+1
            removed_names += [name]
    else:
        del_file(path_counts+name)
        removed_counts[0] = removed_counts[0]+1
        removed_names += [name]

del_file(path_counts+"PG3512")
removed_names += ["PG3512"]
print("deleted Human Genom PG3512")

print("empty files removed: " + str(removed_counts[0]))
print("wrong encoding removed: " + str(removed_counts[0]))
df = pd.DataFrame(removed_names)
df.to_csv('changed_data/empty_counts.csv') 


