#!/bin/bash

#10T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 19G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" ca-HepPh.txt none true 8 "ca-HepPh.txt" true
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> GFPX-12E-ca-HepPh.res
	sleep 1m
done

#100T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 19G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" com-dblp.ungraph.txt none true 8 "com-dblp.ungraph.txt" true
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> GFPX-12E-com-dblp.ungraph.res
	sleep 1m
done

# 1M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 19G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" loc-gowalla_edges.txt none true 8 "loc-gowalla_edges.txt" true
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> GFPX-12E-loc-gowalla_edges.res
	sleep 1m
done

# 10M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 19G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" soc-Slashdot0902.txt none true 8 "soc-Slashdot0902.txt" true
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> GFPX-12E-soc-Slashdot0902.res
	sleep 1m
done

# 100M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 19G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" wiki-Talk.txt none true 16 "wiki-Talk.txt" true
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> GFPX-12E-wiki-Talk.res
	sleep 10m
done


