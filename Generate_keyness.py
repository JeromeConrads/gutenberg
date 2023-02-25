import spacy
import pandas as pd
import numpy as np
from os import listdir, path

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"

count_location = path.join(data_loc,"data/counts/")
count_names = np.array(listdir(count_location))
Document_amount = count_names.shape[0]
all_counts_df = pd.read_csv("changed_data/all_counts.txt",usecols = ["key","document_frequency"],index_col = "key", sep="\t")
i = 1
for PG in count_names:
	txt = pd.read_csv(path.join(count_location,PG),usecols =[0,1], names=["key", "term_freq"], sep="\t")

	#txt will contain the words, term frequency and document frequency
	txt = txt.join(all_counts_df, on = "key")
	#tf-idf
	# calc keyness: w_d = f_w,d * log(|D|/f_w,D)
	# f_w,d = how often w appears in text
	# |D| size of corpus
	# f_w,D = number of documents w appears in Corpus
	# np.around gets used to save disc space significantly
	txt["tf-idf"] =  np.around(txt.term_freq* np.log(Document_amount/txt.document_frequency),2)
	txt = txt.sort_values("tf-idf",ascending=False)
	txt.to_csv(path.join(count_location,PG),columns = ["key","term_freq","tf-idf"], sep="\t",header = None,index = False)
	print(str(PG)+ " saved. "+ str(i) +"/"+str(Document_amount)+" completed")
	i = i+1
