#!/usr/bin/env python3
import sys
import os
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

nltk.download('stopwords', quiet=True)
nltk.download('punkt_tab', quiet=True)
STOPWORDS = set(stopwords.words('english'))
stemmer = PorterStemmer()

# first mapper functionality. preprocess data and pass to the reducer
for _, line in enumerate(sys.stdin):
    try:
        splitted_input_line = line.strip().split("\t")
        doc_id = splitted_input_line[0] 
        title = splitted_input_line[1]
        text = splitted_input_line[2]
        content = text.strip()
        doc_id = (str(doc_id) + "_" + title).replace(" ", "_")
        print(f"DOC_CONTENT\t{doc_id}\t{content}")

        tokens = word_tokenize(text)
        cleaned_tokens = []
        for token in tokens:
            if token.isalnum() and token not in STOPWORDS:
                cleaned_tokens.append(stemmer.stem(token))
        
        doc_length = len(tokens)
        print(f"DOC_LEN\t{doc_id}\t{doc_length}")
        term_freq = {}
        for token in cleaned_tokens:
            term_freq[token] = term_freq.get(token, 0) + 1
        for term, freq in term_freq.items():
            print(f"TERM_FREQ\t{term}\t{doc_id}\t{freq}")
            
    except Exception as e:
        sys.stderr.write(f"Error processing line\n")