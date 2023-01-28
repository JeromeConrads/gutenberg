from pathlib import Path
from typing import List

import pandas as pd
import numpy as np
from os.path import exists
from os import remove

# I remove all entries and that are not completely in english
# successful entries contain just ['en'] in langauge column in metadata.csv


metadata_path = str(Path("metadata/metadata.csv"))
metadata = pd.read_csv(metadata_path, sep=",")
print(metadata.loc[0])

is_en_bitmap = metadata["language"].str.contains("['en']", regex=False)

non_en_metadata = metadata[~ is_en_bitmap]
print(non_en_metadata.id.iloc[0])

non_en_metadata.to_csv("non_en_metadata.txt", sep=",")


# remove from metadate
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

def remove_data(metadataframe):
    file_Paths = ["data/counts/", "data/raw/", "data/text/", "data/tokens/"]
    file_extensions = ["_counts.txt", "_raw.txt", "_text.txt", "_tokens.txt", ]
    successfully_removed = [0, 0, 0, 0]
    for j in range(0, metadataframe.shape[0]):
        for i in range(0, len(file_Paths)):
            full_Path = file_Paths[i] + metadataframe.id.iloc[j] + file_extensions[i]
            print("removed "+ str(metadataframe.id.iloc[j]))
            if exists(full_Path):
                successfully_removed[i] += del_file(full_Path)
    print(successfully_removed)

remove_data(non_en_metadata)
#remove the removed metadata from metadate
clean_metadata = pd.concat([metadata, non_en_metadata, non_en_metadata]).drop_duplicates(keep=False)
clean_metadata.to_csv("clean_metadata.csv", sep=",")
print(clean_metadata.shape)
print(metadata.shape)

