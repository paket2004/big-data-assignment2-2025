#!/bin/bash

INPUT_PATH="/index/data"
log() {
    echo "$1"
}

prepare_data() {
    local directory="$1"

    log "Processing directory: $directory"

    source .venv/bin/activate
    export PYSPARK_DRIVER_PYTHON=$(which python)
    unset PYSPARK_PYTHON

    python -c "
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os
import re

spark = SparkSession.builder.appName('uploading data').master('local').getOrCreate()
input_directory_path = '$directory'
file_data = []

for filename in os.listdir(input_directory_path):
    file_path = os.path.join(input_directory_path, filename)
    if os.path.isdir(file_path): continue
    match = re.match(r'(\d+)_(.+)\\.txt$', filename)
    if match:
        doc_id = match.group(1)
        title = match.group(2).replace('_', ' ')
    else:
        doc_id = str(len(file_data) + 1)
        title = os.path.splitext(filename)[0]
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        file_data.append((doc_id, title, text))
    except Exception as e:
        print(f'Error reading file, continue')

def create_doc(row):
    os.makedirs('data', exist_ok=True)
    filename = 'data/' + sanitize_filename(str(row['id']) + '_' + row['title']).replace(' ', '_') + '.txt'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(row['text'])


df = spark.createDataFrame(file_data, ['id', 'title', 'text'])
df.foreach(create_doc)
df.write.option('sep', '\t').mode('append').csv('/index/data')
spark.stop()
"

    if [ $? -eq 0 ]; then
        log "Preprocessing is done"
        log "Appending files to HDFS with command from prepare_data.sh"
        hdfs dfs -put data /
        hdfs dfs -ls /data
        hdfs dfs -ls /index/data
        return 0
    else
        log "Preprocessing failed"
        return 1
    fi
}


if ! prepare_data "/app/data"; then
                log "Failed to process"
                exit 1
fi

# Situation when we use custom path
if [ "$#" -eq 1 ]; then
    CUSTOM_PATH="$1"

    if [ -e "$CUSTOM_PATH" ]; then
        if [ -d "$CUSTOM_PATH" ]; then
            if ! prepare_data "$CUSTOM_PATH"; then
                log "Failed to process..."
                exit 1
            fi
        elif [ -f "$CUSTOM_PATH" ]; then
            hdfs dfs -put "$CUSTOM_PATH" /data/
        else
            log "Use HDFS path"
        fi
    else
        log "Assuming HDFS path"
    fi
fi

# remove directories from prev runs
hdfs dfs -rm -r /tmp/index/output1
hdfs dfs -rm -r /tmp/index/output2

# make scripts executable
chmod +x $(pwd)/mapreduce/mapper1.py $(pwd)/mapreduce/reducer1.py
chmod +x $(pwd)/mapreduce/mapper2.py $(pwd)/mapreduce/reducer2.py

# First MapReduce
log "Starting MapReduce1"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=4096 \
    -D mapreduce.reduce.java.opts=-Xmx1800m \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/index/output1

if [ $? -ne 0 ]; then
    log "1st MapReduce  failed"
    exit 1
fi

# Second MapReduce
log "Starting MapReduce2"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=4096 \
    -D mapreduce.reduce.java.opts=-Xmx1800m \
    -mapper ".venv/bin/python mapper2.py" \
    -reducer ".venv/bin/python reducer2.py" \
    -input "/tmp/index/output1" \
    -output "/tmp/index/output2"

if [ $? -ne 0 ]; then
    log "2nd MapReduce failed"
    exit 1
fi

# Final cleanup
hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2
