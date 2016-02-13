#! /bin/bash

SRC=./raw/aviation/airline_ontime/

for src in $(find -L $SRC -mindepth 1 -type d); do
  dst=$(echo $src | sed 's#raw/#data/#')
  mkdir -p $dst

  find $src -name "*.zip" | xargs -n1 -P4 -I '{}' unzip '{}' -d $dst
done

find -L ./data/ -name "*.csv" | grep -v concise.csv | xargs -n1 -P4 ./process_csv
