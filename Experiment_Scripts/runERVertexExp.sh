#!/bin/bash

#10T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" ER-10T.graph none true 8 "10TGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> ER-10T.res
	sleep 1m
done

#100T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" ER-100T.graph none true 8 "100TGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> ER-100T.res
	sleep 1m
done

# 1M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" ER-1M.graph none true 16 "1MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> ER-1M.res
	sleep 1m
done

# 10M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" ER-10M.graph none true 32 "10MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> ER-10M.res
	sleep 1m
done

# 100M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=3048 gfp.jar "yarn-cluster" ER-100M.graph none true 512 "100MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> ER-100M.res
	sleep 10m
done
