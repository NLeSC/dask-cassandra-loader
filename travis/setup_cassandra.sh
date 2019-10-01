#!/bin/bash

echo "test script 1"

function cassandra_ready() {
    count=0
    echo "waiting for Cassandra"
    while ! cqlsh -e "describe cluster;" 2>&1 ; do
        echo "waiting for cassandra"
        if [ $count -gt 30 ]
        then
            exit
        fi
        (( count += 1 ))
        sleep 5
    done
    echo "cassandra is ready"
}

cassandra_ready
cqlsh -e "create keyspace dev with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE dev; create table play(code int primary key, title varchar); insert into play (code, title) values (1, 'hello!');"