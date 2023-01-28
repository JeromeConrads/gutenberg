"""
First python File for Bachelor Thesis
contains functions to explore the data
Written by
Jerome Conrads

"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if __name__ == '__main__':

    metadata = pd.read_csv("metadata/metadata.csv")
    # text_sample = pd.read_fwf("data/text/PG100_text.txt", header=None)
    # raw_sample = pd.read_fwf("data/raw/PG100_raw.txt", header=None)
    # counts_sample = pd.read_csv("data/counts/PG100_counts.txt", sep="\t", header=None)
    print(type(metadata), metadata.shape, metadata.dtypes)
    print(metadata["type"].unique())
    #print(metadata["language"].unique())
    #print(metadata["subjects"].unique())
    print(metadata.loc[metadata["type"] != "NaN"])
    metadata2 = metadata.dropna(subset=["type"])
    print(type(metadata2), metadata2.shape, metadata2.dtypes)
    print(metadata2)
