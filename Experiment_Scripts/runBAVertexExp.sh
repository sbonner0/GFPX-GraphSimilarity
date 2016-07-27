#!/bin/bash

#10T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" 10T-BA.graph none true 8 "10TGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> BA-10T.res
	sleep 1m
done

#100T Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" 100T-BA.graph none true 8 "100TGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> BA-100T.res
	sleep 1m
done

# 1M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" 1M-BA.graph none true 16 "1MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> BA-1M.res
	sleep 1m
done

# 10M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=2048 gfp.jar "yarn-cluster" 10M-BA.graph none true 32 "10MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> BA-10M.res
	sleep 1m
done

# 100M Vertices
for value in {1..5}
do
	start=`date +%s`
	spark-submit --packages ml.sparkling:sparkling-graph-operators_2.11:0.0.6 --deploy-mode cluster --master yarn --num-executors 12 --executor-cores 6 --executor-memory 20G --conf spark.yarn.executor.memoryOverhead=3048 gfp.jar "yarn-cluster" 100M-BA.graph none true 512 "100MGraphFingerPrinting" false
	end=`date +%s`
	runtime=$((end-start))
	echo "$runtime" >> BA-100M.res
	sleep 10m
done
