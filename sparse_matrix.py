import numpy as np
from os import listdir, path
from pathlib import Path
import pandas as pd
import scipy
import dask.dataframe as dd
from scipy.sparse import csr_matrix
data_loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"

# input: metadata,data_loc,all_counts name
# files: all PGs_counts.txt in metadata,
# output: sparse matrix of id-itf of all PGs in metadata
def gen_predict(metadata,data_loc = "",):

    Pgs_names = metadata.id
    data_path  = [path.join(data_loc,"data/counts/",str(id + "_counts.txt")) for id in metadata.id]
    #print(data_path)
    ddf = dd.read_csv(data_path, sep="\t", header=None,usecols =[0,2],
                  names=["key", "tf-idf"],dtype ={"key": "string"})
    all_words = pd.read_csv (path.join(data_loc,"data","test_all_counts.txt"),sep="\t",usecols =[0])
    print(all_words)
    for i in range(0,Pgs_names.shape[0]):
        all_words = all_words.merge(ddf.partitions[i].compute(),how= "outer",on= ["key"])
        print(all_words)
    #print(df)
    #df.to_csv("joinedPG")
    #print(df.dtypes)
    print(all_words)
    dd.from_dask_array(ddf.map_partitions(lambda ddf: csr_matrix(ddf)))
    sparse_ddf = ddf.compute()
    print(sparse_ddf)
    
    sparse_df = df.T.fillna(0).astype(pd.SparseDtype("float64",0))
    print(sparse_df)
    csr_sparse_matrix = sparse_df.sparse.to_coo().tocsr()
    print(csr_sparse_matrix)

    from sklearn.linear_model import LogisticRegression
    classifier = LogisticRegression()
    classifier.fit(csr_sparse_matrix,[0,1])
    predicted = classifier.predict(csr_sparse_matrix)
    print(predicted)
    

metadata = pd.read_csv(path.join(data_loc,"metadata","test_metadata.csv"))
gen_predict(metadata,data_loc = data_loc)

