#!/bin/bash

pip3 install pytz
cqlsh=docker run --network="host" --rm -ti cassandra cqlsh -e

function cassandra_ready() {
    count=0
    echo "waiting for Cassandra"
    while ! cqlsh "describe cluster;" 2>&1 ; do
        echo "waiting for cassandra"
        if [ $count -gt 30 ]
        then
            exit
        fi
        (( count += 1 ))
        sleep 1
    done
    echo "cassandra is ready"
}

cassandra_ready
cqlsh "create keyspace dev with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE dev; create table play(code int primary key, title varchar); insert into play (code, title) values (1, 'hello!');"

cqlsh "USE dev; create table tab1(id int, year int, month int, day int, timest timestamp, lat float, lon float, PRIMARY KEY((id, year, month)));"

cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721599000, 53.330097, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721597000, 53.330098, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721595000, 53.330097, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721593000, 53.330097, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721591000, 53.330098, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721589000, 53.330098, 6.932023);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721587000, 53.330098, 6.932025);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721585000, 53.330098, 6.932025);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721583000, 53.330098, 6.932025);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721581000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721579000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721577000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721575000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721573000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721571000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721569000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721567000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721565000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721563000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721561000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721559000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721557000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721555000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721553000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721551000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721549000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721547000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721545000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721543000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541721541000, 53.330098, 6.932027);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635258000, 52.660796, 5.546034);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635256000, 52.660729, 5.546014);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635254000, 52.660663, 5.545994);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635252000, 52.660598, 5.545976);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635250000, 52.660529, 5.545954);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635248000, 52.660464, 5.545934);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635246000, 52.660398, 5.545914);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635244000, 52.660331, 5.545894);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635242000, 52.660264, 5.545873);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635240000, 52.660199, 5.545854);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635238000, 52.660133, 5.545833);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635236000, 52.660066, 5.545813);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635234000, 52.660001, 5.545793);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635232000, 52.659934, 5.545773);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635230000, 52.659868, 5.545753);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635228000, 52.659801, 5.545733);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635226000, 52.659736, 5.545711);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635224000, 52.659669, 5.545693);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635222000, 52.659603, 5.545671);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635220000, 52.659538, 5.545651);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635218000, 52.659471, 5.545629);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635216000, 52.659404, 5.545608);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635214000, 52.659338, 5.545588);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635212000, 52.659273, 5.545568);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635210000, 52.659208, 5.545546);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635208000, 52.659141, 5.545526);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635206000, 52.659074, 5.545506);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635204000, 52.659009, 5.545484);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635202000, 52.658941, 5.545484);"
cqlsh "USE dev; insert into tab1 (id, year, month, day, timest, lat, lon) values (18, 2018, 11, 8, 1541635200000, 52.658875, 5.545464);"
