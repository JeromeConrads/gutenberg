import spacy
import pandas as pd
import numpy as np
from os import listdir

count_location = "data/counts/"
count_names = np.array(listdir(count_location))


def load_counts_text(location):
    text = pd.read_csv(location, sep="\t", header=None)
    if text.shape[1] != 2:
        return text
    text.columns = ["0", "1"]
    text = text.set_index("0")
    return text


total_corpus = load_counts_text("total_counts.txt")
txt = load_counts_text(count_location + count_names[0])

print(total_corpus.sum()[0])
