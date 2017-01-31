#!/bin/bash

hjs -files /home/ue225/bigdata2017/lab2/src -mapper src/map.sh -reducer src/reduce.sh -input /user/ecc290/matbig.txt -output /user/ue225/mm.out
hfs -getmerge mm.out mm.out
hfs -rm -r mm.out