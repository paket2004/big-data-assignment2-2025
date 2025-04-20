#!/usr/bin/env python3
import sys
from collections import defaultdict

doc_lengths = {}
texts = {}
term_frequencies = defaultdict(dict)

# process input from mapper1
for line in sys.stdin:
    try:
        input = line.strip()
        input_splitted = input.split('\t')
        
        if len(input_splitted) < 3:
            continue
        if input_splitted[0] == "DOC_LEN":
            doc_lengths[input_splitted[1]] = int(input_splitted[2])
        elif input_splitted[0] == "TERM_FREQ":
            term_frequencies[input_splitted[1]][input_splitted[2]] = int(input_splitted[3])
        elif input_splitted[0] == "DOC_CONTENT":
            texts[input_splitted[1]] = input_splitted[2]
            
    except Exception as e:
        sys.stderr.write(f"Error processing line {line}: {str(e)}\n")

# output to reducer2
for term in term_frequencies.keys():
    print(f"VOCAB\t{term}")

for doc_id, text in texts.items():
    print(f"DOC_CONTENT\t{doc_id}\t{text}")


for doc_id, length in doc_lengths.items():
    print(f"DOC_LEN\t{doc_id}\t{length}")

for term, doc_freqs in term_frequencies.items():
    print(f"DOC_FREQ\t{term}\t{len(doc_freqs)}")

for term, doc_freqs in term_frequencies.items():
    for doc_id, freq in doc_freqs.items():
        print(f"TERM_FREQ\t{term}\t{doc_id}\t{freq}")

