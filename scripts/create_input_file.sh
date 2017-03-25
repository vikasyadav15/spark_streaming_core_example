#!/bin/bash

for f in {1..10}; do
        t=$f
head_val=3
     #head_val=$(( $f * 10 ))
      perl -MList::Util -e 'print List::Util::shuffle <>' test >test_sort
      head -n $head_val test_sort  >output
     cat v >> output
      awk -v var=$t -F, '{$1=var FS $1;}1' OFS=, output  >/Users/vikasyadav/Code_Repo/spark_in_action/dstream_input/$f.txt
     sleep 4
done
