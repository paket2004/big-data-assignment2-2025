#!/usr/bin/env python3
import sys
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

try:
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("USE bigdata")
    
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch_size = 0
    max_batch_size = 1
    
    # process input from mapper2
    for line in sys.stdin:
        try:
            reducer_input = line.strip()
            splitted_input = reducer_input.split('\t')
            
            if len(splitted_input) < 2:
                continue
            
            if splitted_input[0] == "VOCAB":
                batch.add(session.prepare("INSERT INTO vocab (term) VALUES (?)"), (splitted_input[1],))
                batch_size += 1
            
            elif splitted_input[0] == "DOC_LEN":
                batch.add(session.prepare("INSERT INTO doc_len (doc_id, doc_len) VALUES (?, ?)"), (splitted_input[1], int(splitted_input[2])))
                batch_size += 1
            
            elif splitted_input[0] == "DOC_CONTENT":
                batch.add(session.prepare("INSERT INTO doc_content (doc_id, doc_content) VALUES (?, ?)"), (splitted_input[1],  splitted_input[2]))
                batch_size += 1

            
            elif splitted_input[0] == "DOC_FREQ":
                batch.add(session.prepare("INSERT INTO doc_freq (term, doc_freq) VALUES (?, ?)"), (splitted_input[1], int(splitted_input[2])))
                batch_size += 1
            
            elif splitted_input[0] == "TERM_FREQ":
                batch.add(session.prepare("INSERT INTO term_freq (term, doc_id, term_freq) VALUES (?, ?, ?)"), (splitted_input[1], splitted_input[2], int(splitted_input[3])))
                batch_size += 1

            
            if batch_size >= max_batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_size = 0
                
        except Exception as e:
            sys.stderr.write(f"Error processing line {line}: {str(e)}\n")
    
    # execute remaining items in batch
    if batch_size > 0:
        session.execute(batch)
    
except Exception as e:
    sys.stderr.write(f"Fatal error in REDUCER2")
    sys.exit(1)
    
finally:
    if 'cluster' in locals():
        cluster.shutdown()