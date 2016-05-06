#!/bin/bash

# REQUIRED ENVIRONMENT VARIABLES
# INPUT           - input FILE (list of directories not implemented yet)
# OUTPUT          - output file
# MAPPER          -
# REDUCER         -
# TMP_DIR         - directory for storing files from parallel
# NUM_OF_REDUCERS - number for workers, both for map and reduce

INPUT_SIZE=`stat --printf="%s" $INPUT`
BLOCK_SIZE=`printf %d $(echo "$INPUT_SIZE / $NUM_OF_REDUCERS / 1.9" | bc -l)`
# make a little bit less blocks that twice the NUM_OF_REDUCERS. To handle the situation when few blocks take more time than the others.
echo INPUT_SIZE=$INPUT_SIZE
echo NUM_OF_REDUCERS=$NUM_OF_REDUCERS
echo BLOCK_SIZE=$BLOCK_SIZE
if [ -n "$TMP_DIR" ]; then rm -rf $TMP_DIR/*; fi

echo MAPPER=$MAPPER
echo "parallel --pipe --tmpdir $TMP_DIR --files -j$NUM_OF_REDUCERS --block=$BLOCK_SIZE $MAPPER | LC_ALL=C sort -k 1,1 -t $'\t'"
cat $INPUT \
  | parallel --pipe --tmpdir $TMP_DIR --files -j$NUM_OF_REDUCERS --block=$BLOCK_SIZE \
             "$MAPPER | LC_ALL=C sort -k 1,1 -t $'\t'";

TMP_SIZE=`du -b $TMP_DIR | cut -f1`
BLOCK_SIZE=`echo "$TMP_SIZE / $NUM_OF_REDUCERS / 1.9" | bc -l`
LC_ALL=C sort -k 1,1 -t $'\t' -m $TMP_DIR/*.par \
  | awk 'BEGIN {last = ""; FS = "\t"}; { if (last != $1) { print "=SEPARATOR=\n"; last = $1 }; print}; END { print "\n" }' \
  | parallel --pipe -k -j$NUM_OF_REDUCERS --block=$BLOCK_SIZE --recend="=SEPARATOR=" "grep -v '^$' | grep -v '^=SEPARATOR=$' | $REDUCER" \
  > $OUTPUT
