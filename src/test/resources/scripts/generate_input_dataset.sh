#!/bin/bash

FILE_NAME=ADDIT1ZB_SRES_SVR002FLW_C_20200428_155139.json
for (( n=1; n <= 40000; n++))
  do
    cp ${FILE_NAME} TAG${n}_20200428_155139.json
  done
