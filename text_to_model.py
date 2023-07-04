import pandas as pd
import dask.dataframe as dd
from os import listdir, path
import spacy
import string

from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.en import English
from sklearn import metrics
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from sklearn.base import TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier

# Create our list of punctuation marks
punctuations = string.punctuation

# Create our list of stopwords
nlp = spacy.load('en_core_web_sm')
stop_words = spacy.lang.en.stop_words.STOP_WORDS

# Load English tokenizer, tagger, parser, NER and word vectors
parser = English()

# Creating our tokenizer function
def spacy_tokenizer(sentence):
    # Creating our token object, which is used to create documents with linguistic annotations.
    mytokens = parser(sentence)

    # Lemmatizing each token and converting each token into lowercase
    mytokens = [ word.lemma_.lower().strip() if word.lemma_ != "-PRON-" else word.lower_ for word in mytokens ]

    # Removing stop words
    mytokens = [ word for word in mytokens if word not in stop_words and word not in punctuations ]

    # return preprocessed list of tokens
    return mytokens
    
# Custom transformer using spaCy
class predictors(TransformerMixin):
    def transform(self, X, **transform_params):
        # Cleaning Text
        return [clean_text(text) for text in X]

    def fit(self, X, y=None, **fit_params):
        return self

    def get_params(self, deep=True):
        return {}

# Basic function to clean the text
def clean_text(text):
    # Removing spaces and converting text into lowercase
    return text.strip().lower()
    
bow_vector = CountVectorizer( ngram_range=(1,1))
tfidf_vector = TfidfVectorizer()

lst = ["'Science fiction'", "'Short stories'", "'Fiction'"]

data_loc ="/media/jerome/Bacheler/Gutenbergfiles/smalldata"
metadata = pd.read_csv(path.join(data_loc,"metadata","train_metadata.csv"))
data_path  = [path.join(data_loc,"data/tokens/",str(id + "_tokens.txt")) for id in metadata.id]

X_train = dd.read_csv(data_path, header=None,names= ["text"]).dropna()
# the features we want to analyze

# each df is 1 book containing 1 word per row
# join them all together into 1 row
def squeeze_row_f(df):
    print(df)
    return pd.DataFrame([' '.join(df["text"])])
X_train = X_train.map_partitions(squeeze_row_f)
y_train = metadata[[ 'subjects']]


top3_genres = ["'Science fiction'", "'Short stories'", "'Fiction'"]

for i, substring in enumerate(top3_genres):
    y_train['subjects'] = y_train['subjects'].apply(lambda x: str(i) if substring in x else x)
y_train['subjects'] = pd.to_numeric(y_train['subjects'])
print(y_train)

metadata = pd.read_csv(path.join(data_loc,"metadata","test_metadata.csv"))
data_path  = [path.join(data_loc,"data/tokens/",str(id + "_tokens.txt")) for id in metadata.id]

X_test = dd.read_csv(data_path, header=None,names= ["text"]).dropna()
X_test = X_test.map_partitions(squeeze_row_f)
y_test = metadata[['id', 'subjects']]



for i, substring in enumerate(top3_genres):
    y_test['subjects'] = y_test['subjects'].apply(lambda x: str(i) if substring in x else x)
y_test['subjects'] = pd.to_numeric(y_test['subjects'])


print("Compute X train")
X_train = X_train.compute()

#X_train, X_test, y_train, y_test = train_test_split(X, ylabels, test_size=0.3)


# Logistic Regression Classifier

classifier = LogisticRegression()
#print(tfidf_vector.fit_transform(X_train))
#print(tfidf_vector.get_feature_names_out())
# Create pipeline using Bag of Words
pipe = Pipeline([('vectorizer', tfidf_vector),
                 ('classifier', classifier)])
# model generation
print(X_train[0])
print(y_train)
pipe.fit(X_train[0],y_train['subjects'])
print(pipe)

# Predicting with a test dataset
print("Compute X test")
X_test= X_test.compute()
predicted = pipe.predict(X_test[0])
# Model Accuracy
print("Logistic Regression Accuracy:",metrics.accuracy_score(y_test['subjects'], predicted))
print(metrics.classification_report(y_test['subjects'], predicted, target_names=top3_genres))

clf = RandomForestClassifier(n_estimators=10)

pipe = Pipeline([('vectorizer', tfidf_vector),
                 ('classifier', clf)])
                 
pipe.fit(X_train[0],y_train['subjects'])      
predicted = pipe.predict(X_test[0])

print("RandomForestClassifier Accuracy:",metrics.accuracy_score(y_test['subjects'], predicted))
print(metrics.classification_report(y_test['subjects'], predicted, target_names=top3_genres))

clf = svm.SVC()  

pipe = Pipeline([('vectorizer', tfidf_vector),
                 ('classifier', clf)])
                 
pipe.fit(X_train[0],y_train['subjects'])      
predicted = pipe.predict(X_test[0])

print("Support Vector Machine Accuracy:",metrics.accuracy_score(y_test['subjects'], predicted))
print(metrics.classification_report(y_test['subjects'], predicted, target_names=top3_genres))     
