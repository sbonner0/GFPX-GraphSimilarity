#!/bin/bash
# Graph Gen Script. Please run from root of Snap install dir and set directories accordingly.

NUMVERTICES100=100000
NUMVERTICES1M=1000000

DATALOCATION=

for i in {1..50}
do
	# ER Graphs
   examples/graphgen/graphgen -o:er-100k-$i.txt -g:e -n:$NUMVERTICES100 -m:200000
   examples/graphgen/graphgen -o:er-1m-$i.txt -g:e -n:$NUMVERTICES1M -m:2000000

   # BA Graphs
   examples/graphgen/graphgen -o:ba-100k-k2-$i.txt -g:b -n:$NUMVERTICES100 -k:2
   examples/graphgen/graphgen -o:ba-100k-k4-$i.txt -g:b -n:$NUMVERTICES100 -k:4
   examples/graphgen/graphgen -o:ba-1m-k2-$i.txt -g:b -n:$NUMVERTICES1M -k:2
   examples/graphgen/graphgen -o:ba-1m-k4-$i.txt -g:b -n:$NUMVERTICES1M -k:4

   # WS Graphs
   examples/graphgen/graphgen -o:ws-100k-k4-p-0.1-$i.txt -g:w -n:$NUMVERTICES100 -k:4 -p:0.1
   examples/graphgen/graphgen -o:ws-1m-k4-p-0.1-$i.txt -g:w -n:$NUMVERTICES1M -k:4 -p:0.1

   # FF Graphs
   examples/forestfire/forestfire -o:ff-100k-f20-b40-$i.txt -n:$NUMVERTICES100 -f:0.2 -b:0.4
   examples/forestfire/forestfire -o:ff-1m-f20-b40-$i.txt -n:$NUMVERTICES1M -f:0.2 -b:0.4

done
