import spacy
import pandas as pd
import numpy as np
from os import listdir, path


# input: data_loc,metadata, all_counts.txt loc.
# files: all_counts.txt,all PGs_counts.txt in metadata,
# output: nothing, adds tf-idf to PGs_counts.txt
def gen_keyness(metadata,data_loc = "" , all_counts_loc = "data/all_counts.txt"):
    count_location = path.join(data_loc,"data/counts/")
    count_names = np.array(metadata.id)
    Document_amount = count_names.shape[0]
    print(data_loc)
    print("Document_amount: "+ str(Document_amount))
    all_counts_df = pd.read_csv(path.join(data_loc,all_counts_loc),usecols = ["key","document_frequency"],index_col = "key", sep="\t")
    i = 1
    for PG in count_names:
	    txt = pd.read_csv(path.join(count_location,str(PG +"_counts.txt")),usecols =[0,1], names=["key", "term_freq"], sep="\t")

	    #txt will contain the words, term frequency and document frequency
	    #TODO replace very expensive join operation
	    txt = txt.join(all_counts_df, on = "key")
	    #tf-idf
	    # calc keyness: w_d = f_w,d * log(|D|/f_w,D)
	    # f_w,d = how often w appears in text
	    # |D| size of corpus
	    # f_w,D = number of documents w appears in Corpus
	    # np.around gets used to save disc space significantly
	    
	    txt["tf-idf"] =  np.around(txt.term_freq* np.log(Document_amount/txt.document_frequency),2)
	    txt = txt.sort_values("tf-idf")
	    txt.to_csv(path.join(count_location,str(PG+ "_counts.txt")),columns = ["key","term_freq","tf-idf"], sep="\t",header = None,index = False)
	    print(str(PG)+ " saved. "+ str(i) +"/"+str(Document_amount)+" completed")
	    i = i+1

loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"
md = pd.read_csv(path.join(loc,"metadata/test_metadata.csv"))
allcounts = "data/test_all_counts.txt"
print(md)

gen_keyness( metadata= md,data_loc = loc , all_counts_loc = allcounts )
loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"
md = pd.read_csv(path.join(loc,"metadata/train_metadata.csv"))
allcounts = "data/train_all_counts.txt"
print(md)

gen_keyness( metadata= md,data_loc = loc , all_counts_loc = allcounts )
