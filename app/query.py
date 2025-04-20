#!/usr/bin/env python3

import sys
import math
from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from cassandra.cluster import Cluster
from operator import add
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re

nltk.download('stopwords', quiet=True)
nltk.download('punkt_tab', quiet=True)
STOPWRODS = set(stopwords.words('english'))
stemmer = PorterStemmer()

def calculate_bm25_score(query_parsed_in_tokens, doc_id, doc_len, average_doc_len, term_freq, doc_freq, total_docs):
    # k1 and B from https://kmwllc.com/index.php/2020/03/20/understanding-tf-idf-and-bm-25/
    K1 = 1.2
    B = 0.75
    bm25_score = 0.0
    for term in query_parsed_in_tokens:
        if term in term_freq and doc_id in term_freq[term]:
            tf = term_freq[term][doc_id]
            print(f"tf {tf}")
            df = doc_freq.get(term, 0)
            print(f"df{df}")
            if df > 0:
                idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)
                bm25_score += idf * ((tf * (K1 + 1)) / (tf + K1 * (1 - B + B * doc_len / average_doc_len)))
                print(f"score{bm25_score}")
    return bm25_score

def main():
    
    query = " ".join(sys.argv[1:])
    
    spark = SparkSession.builder \
        .appName("retrieve_relevant_documents") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()
    sc = spark.sparkContext

    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('bigdata')
    
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', query.lower())
    tokens = word_tokenize(text)
    query_parsed_in_tokens = []
    for token in tokens:
        if token.isalnum() and token not in STOPWRODS:
            query_parsed_in_tokens.append(stemmer.stem(token))
    
    
    try:
        doc_len = {}
        doc_table_entries = list(session.execute(f"SELECT * FROM doc_len"))
        print(len(doc_table_entries))
        for entry in doc_table_entries:
            doc_len[entry.doc_id] = entry.doc_len
        
        doc_freq = {}
        doc_freqs_entries = list(session.execute(f"SELECT * FROM doc_freq"))
        print(len(doc_freqs_entries))
        for entry in doc_freqs_entries:
            doc_freq[entry.term] = entry.doc_freq
        
        
        doc_сontent = {}
        doc_content_entries = list(session.execute(f"SELECT * FROM doc_content"))
        for entry in doc_content_entries:
            doc_сontent[entry.doc_id] = entry.doc_content
        
        term_freq = {}
        for term in query_parsed_in_tokens:
            rows = session.execute(f"SELECT doc_id, term_freq FROM term_freq WHERE term = '{term}'")
            term_freq[term] = {row.doc_id: row.term_freq for row in rows}
        
        average_doc_len = 0
        if len(doc_len) > 0:
            average_doc_len = sum(doc_len.values()) / len(doc_len)
        
        doc_ids_rdd = sc.parallelize(list(doc_len.keys()))
        
        doc_bm25_result = doc_ids_rdd.map(
            lambda doc_id: (
                doc_id, 
                calculate_bm25_score(
                    query_parsed_in_tokens, 
                    doc_id, 
                    doc_len.get(doc_id, 0), 
                    average_doc_len, 
                    term_freq, 
                    doc_freq, 
                    len(doc_len)
                )
            )
        )
        
        relevant_documents = doc_bm25_result.sortBy(lambda x: -x[1]).take(5)
        
        print(f"\nTop 5 results for: '{query}'")
        print("--------------------------------")
        
        if not relevant_documents:
            print("Something went wrong in some part of the program. Please, debug it")
        else:
            for i, (doc_id, score) in enumerate(relevant_documents, 1):
                print(f"{i}.Document ID: {doc_id}")
                print(f"Relevance score: {score}")
                print(f"Beginning of the file: {doc_сontent[doc_id][:100]}")
                print()
        
    finally:
        cluster.shutdown()
        spark.stop()

if __name__ == "__main__":
    main()