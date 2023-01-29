import spacy
import pandas as pd
import numpy as np
from os import listdir, path
from pathlib import Path

# nlp = spacy.load("en_core_web_sm")
# text = """spaCy is an open-source software library for advanced natural language processing,
# written in the programming languages Python and Cython. The library is published under the MIT license
# and its main developers are Matthew Honnibal and Ines Montani, the founders of the software company #Explosion."""
# doc = nlp(text)
# print(doc.ents)
count_location = "data/counts/"
count_names = np.array(listdir(count_location))


def load_counts_text(count_name):
    count_name = str(Path(count_name))
    print(count_name)
    txt = pd.read_csv(count_name, sep="\t", header=None,encoding = "utf-8")
    if txt.shape[1] != 2: return txt
    txt.columns = ["0", "1"]
    txt = txt.set_index("0")
    return txt


# sums up the counts
def merge_counts(total_corpus1, one_text1):
    total_corpus1 = pd.merge(total_corpus1, one_text1, how="outer", on=["0"])
    total_corpus1 = total_corpus1.sum(axis=1).to_frame()
    return total_corpus1


total_corpus = load_counts_text(count_location + count_names[0])

empty = 0
not_utf = 0
for i in range(1, count_names.shape[0]):
    filepath = str(Path(count_location + count_names[i]))
    if path.getsize(filepath) > 10:
        one_text = load_counts_text(count_location + count_names[i])
        if one_text.shape[0] > 20:
            total_corpus = merge_counts(total_corpus, one_text)
            print("books processed: " + str(i) + " " + str(count_names[i]))
        else:
            print("Not UTF8: " + str(count_names[i]))
            empty = empty + 1
    else:
        print("empty file : " + str(count_names[i]))
        not_utf = not_utf + 1
    if i % 500 == 0:
        total_corpus.to_csv("data/total_counts/part_counts." + str(i) + ".txt", header=False, sep="\t")
        i = i + 1
        total_corpus = load_counts_text(count_location + count_names[i])

print("total empty: " + str(empty))
print("total not_utf: " + str(not_utf))
total_corpus.to_csv("data/total_counts/part_counts." + str(i) + ".txt", header=False, sep="\t")
print("Now Condensing rest files")

count_location = "data/total_counts/"
count_names = np.array(listdir(count_location))

total_corpus = load_counts_text(count_location + count_names[0])
for i in range(1, count_names.shape[0]):
    filepath = str(Path(count_location + count_names[i]))
    one_text = load_counts_text(filepath)
    total_corpus = merge_counts(total_corpus, one_text)
    print("merged: " + str(count_names[i]))
total_corpus.to_csv("data/total_counts/all_counts.txt", header=False, sep="\t")
