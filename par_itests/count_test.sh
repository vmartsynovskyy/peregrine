#!/bin/bash
trap "kill 0" EXIT

for i in $(seq 4 6); do
    ../bin/count ../data/citeseer ${i}-motifs 4 master | tail -6 > test_par_res &
    ../bin/count ../data/citeseer ${i}-motifs 4 slave &> /dev/null &
    ../bin/count ../data/citeseer ${i}-motifs 4 slave &> /dev/null &
    ../bin/count ../data/citeseer ${i}-motifs 4 slave &> /dev/null &

    wait

    ../bin/count ../data/citeseer ${i}-motifs 16 | tail -6 > test_single_res

    if diff test_single_res test_par_res; then
        echo -e "\xE2\x9C\x94 ${i}-motifs parallel"
    else
        echo -e "\xE2\x9D\x8C ${i}-motifs parallel"
    fi
done

