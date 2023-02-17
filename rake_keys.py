import pandas as pd
import numpy as np
from os import listdir, path
import dask.dataframe as dd
import glob
from pathlib import Path
from rake_nltk import Rake

#import nltk
#nltk.download('stopwords')


data_loc ="/media/jerome/Bacheler/Gutenbergfiles/"


count_location = path.join(data_loc,"data/text/")
count_names = np.array(listdir(count_location))

location = path.join(count_location,count_names[400])
print(location)
txt = pd.read_csv(location, header=None,sep=".",encoding = "utf-8",on_bad_lines='skip')
print(txt.values)
print(count_names[1])
#txt = txt.words.str.cat(sep=' ')
r = Rake()
r.extract_keywords_from_sentences(txt)
r.get_ranked_phrases()
print(r.get_ranked_phrases()[:20])
