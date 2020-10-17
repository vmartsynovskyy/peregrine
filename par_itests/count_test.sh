#!/bin/bash

trap "kill 0" EXIT

../bin/count ../data/citeseer 4-motifs 4 m &
../bin/count ../data/citeseer 4-motifs 4 &
../bin/count ../data/citeseer 4-motifs 4 &
../bin/count ../data/citeseer 4-motifs 4 &

wait

