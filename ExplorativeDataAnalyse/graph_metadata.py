import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd
import scipy
import dask.dataframe as dd
from scipy.sparse import csr_matrix
import matplotlib.pyplot as plt

data_loc ="/media/jerome/Bacheler/Gutenbergfiles"

# input: metadata,data_loc,all_counts name
# files: all PGs_counts.txt in metadata,
# output: sparse matrix of id-itf of all PGs in metadata
def plot_language(metadata,data_loc = "",):

    languages= metadata.language.value_counts()

    other_count = languages[10:].sum()
    languages = pd.concat([languages[:10], pd.Series(other_count, index=['Other'])])



    languages.plot(kind="bar")
    plt.xlabel('Unique Languages')
    plt.ylabel('log Frequency')
    plt.xticks(rotation=0) 
    
    for x, y in enumerate(languages):
        plt.annotate(str(y), xy=(x, y), ha='center', va='bottom')

    plt.show() # bar graph showing the amount of each unique language tag
    print(languages)
    
def plot_authors(metadata,data_loc = "",):

    authors = metadata.author.value_counts().value_counts()

    # plot as pie chart
    authors.plot.pie(startangle=90)
    plt.axis('equal')
    plt.show() # cake graph showing how many books a author writes in %

    print(authors.unique())

metadata = pd.read_csv(path.join(data_loc,"metadata","metadata.csv"))
#plot_language(metadata,data_loc = data_loc)
plot_authors(metadata,data_loc = data_loc)


