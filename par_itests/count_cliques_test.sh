#!/bin/bash
trap "kill 0; rm test_single_res test_par_res" EXIT

for i in $(seq 3 10); do
    ../bin/count ../data/citeseer ${i}-cliques 4 master 4 | tail -1 > test_par_res &
    ../bin/count ../data/citeseer ${i}-cliques 4 127.0.0.1 &> /dev/null &
    ../bin/count ../data/citeseer ${i}-cliques 4 127.0.0.1 &> /dev/null &
    ../bin/count ../data/citeseer ${i}-cliques 4 127.0.0.1 &> /dev/null &

    wait

    ../bin/count ../data/citeseer ${i}-cliques 16 | tail -1 > test_single_res

    if diff test_single_res test_par_res; then
        echo -e "\xE2\x9C\x94 ${i}-cliques parallel"
    else
        echo -e "\xE2\x9D\x8C ${i}-cliques parallel"
        exit 1
    fi
done

