from sklearn.feature_extraction.text import HashingVectorizer

corpus = [

    'This is the first document.',

    'This document is the second document.',

    'And this is the third one.',

    'Is this the first document?',

]

vectorizer = HashingVectorizer(n_features=2**4)

X = vectorizer.fit_transform(corpus)

print(X)
