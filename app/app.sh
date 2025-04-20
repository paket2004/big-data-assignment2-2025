#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt

# Package the virtual env.
venv-pack -o .venv.tar.gz

echo "Starting cassandra"
until cqlsh cassandra-server -e "describe keyspaces" > /dev/null 2>&1; do
  echo "Wait a bit, some issues with initializing occured"
  sleep 3
done
echo "CASSANDRA INITIALIZED"

# Initialize Cassandra schema
cat > /tmp/create_cass_keyspace_ant_tables.cql << EOF
CREATE KEYSPACE IF NOT EXISTS bigdata
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE bigdata;

CREATE TABLE IF NOT EXISTS vocab (
    term text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS doc_len (
    doc_id text PRIMARY KEY,
    doc_len int
);

CREATE TABLE IF NOT EXISTS doc_content (
    doc_id text PRIMARY KEY,
    doc_content text
);

CREATE TABLE IF NOT EXISTS doc_freq (
    term text PRIMARY KEY,
    doc_freq int
);

CREATE TABLE IF NOT EXISTS term_freq (
    term text,
    doc_id text,
    term_freq int,
    PRIMARY KEY (term, doc_id)
);

EOF

cqlsh cassandra-server -f /tmp/create_cass_keyspace_ant_tables.cql

# Collect data
bash prepare_data.sh
# Run the indexer
bash index.sh
# Run the ranker
bash search.sh "Chat gpt transformer"