import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators._
import java.util.Calendar
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}

object GraphFingerprint {

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = { if (a._2 > b._2) a else b }
  
 
  def featureCreation(vertexInfo: VertexRDD[Double], sdf: SQLContext): DataFrame = {
    import sdf.implicits._
    
    // Function to aggregate the vertex information to create the feature vector. 
    val vertexInfoDF: DataFrame = vertexInfo.toDF().persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    val mean: DataFrame = vertexInfoDF.agg("_2" -> "mean")
    val sd: DataFrame = vertexInfoDF.agg("_2" -> "stddev")
    val min: DataFrame = vertexInfoDF.agg("_2" -> "min")
    val max: DataFrame = vertexInfoDF.agg("_2" -> "max")
    val skew: DataFrame = vertexInfoDF.agg("_2" -> "skewness")
    val kurt: DataFrame = vertexInfoDF.agg("_2" -> "kurtosis")
    val vari: DataFrame = vertexInfoDF.agg("_2" -> "variance")
  
    val joinedStats: DataFrame = sd.join(mean).join(min).join(max).join(skew).join(kurt).join(vari)
    vertexInfoDF.unpersist(blocking=true)
    
    return joinedStats
  }
  
  def extractGlobalFeatures(g: Graph[Int, Int], sdfa: SQLContext, RunFull: Boolean = false): DataFrame = {
    
    import sdfa.implicits._
  
    // Count number of edges and vertices
    val numEdges: Long = g.numEdges
    val numNodes: Long = g.numVertices
    
    // Compute the maximum and average degree
    val maxDeg: Int = g.degrees.reduce(max)._2
    val avgDeg: Double = g.degrees.map(_._2).sum / numNodes
    
    // Check if all features are needed
    if(RunFull)
    {
      println("Runing Full Global Extraction")
      // Number of triangles in graph
      val numTriangles: Double = g.triangleCount().vertices.map(x => x._2).sum() / 3
  
      // Connected components
      val Components: VertexRDD[VertexId] = g.connectedComponents().vertices.persist(StorageLevel.MEMORY_AND_DISK_SER)
      val numComp: Long = Components.map(x => x._2).distinct().count()
      val NumVerticesInLCC: Int = Components.map(x => (x._2, 1)).reduceByKey(_ + _).map(x => x._2).max()
      val PercentageInLCC: Double = (NumVerticesInLCC / numNodes) * 100.0
      Components.unpersist(blocking=true)
      
      val gFeats = Seq((numNodes.toDouble, numEdges.toDouble, numComp.toDouble, NumVerticesInLCC.toDouble, PercentageInLCC.toDouble, maxDeg.toDouble, avgDeg.toDouble, numTriangles))
       .toDF("numNodes", "numEdges", "numComp", "NumVerticesInLCC", "PercentageInLCC", "maxDeg", "avgDeg", "Tri")
       
       return gFeats
    }
    else
    {
      println("Runing  Global Extraction")
       val gFeats = Seq((numNodes.toDouble, numEdges.toDouble , maxDeg.toDouble, avgDeg.toDouble))
       .toDF("numNodes", "numEdges", "maxDeg", "avgDeg")
       
       return gFeats
    }
  }

  def extractVertexFeatures(g: Graph[Int, Int], sdf: SQLContext): DataFrame = {
    import sdf.implicits._
    
    // Page Rank RDD 
    val pagerankGraph: Graph[Double, Double] = g.pageRank(0.00015)
    val pageRankRDD: VertexRDD[Double] = pagerankGraph.vertices
    
    // Total Degree RDD
    val degreeRDD: VertexRDD[Int] = g.degrees
     
    // Local Clustering Score
    val clusteringGraph: Graph[Double, Int] = g.localClustering(VertexMeasureConfiguration(treatAsUndirected=true))
    val localCentralityRDD: VertexRDD[Double] = clusteringGraph.vertices
    
    // Eigen Vector Score
    val eigenvectorRDD: VertexRDD[Double] = g.eigenvectorCentrality(VertexMeasureConfiguration(treatAsUndirected=true)).vertices
    val eigenVecDF: DataFrame = featureCreation(eigenvectorRDD, sdf)
    eigenvectorRDD.unpersist(blocking=true)
    
    val degreeGraph: Graph[Int, Int] = g.outerJoinVertices(degreeRDD) { (id, oldAttr, degOpt) =>
        degOpt match {
          case Some(deg) => deg
          case None => 0 // No outDegree means zero outDegree
        }
    }
    
    // Calculte the number of two hop away neigbours
    val twoHopAway: VertexRDD[Double] = degreeGraph.aggregateMessages[Double](
      triplet => { // Map Function
      // Send the degree of each vertex to the reduce function. Not sure if both things are needed here. 
        triplet.sendToDst(triplet.srcAttr)
        triplet.sendToSrc(triplet.dstAttr)
      },
      // Sum the degree values
        (a, b) => (a + b) // Reduce Function
    )
    
    val twoHopDF: DataFrame  = featureCreation(twoHopAway , sdf)
    val degreeDF: DataFrame = featureCreation(degreeRDD.mapValues(x => x.toDouble), sdf)
    
    // Calculate the average clustering score of the each 
    val totalClusteringScore: VertexRDD[(Int, Double)] = clusteringGraph.aggregateMessages[(Int, Double)](        
      triplet => { // Map Function
      // Send the local clustering score and a counter to get the mean
        triplet.sendToDst(1, triplet.srcAttr)
        triplet.sendToSrc(1, triplet.dstAttr)
      },
      // Add the local clustering scores
        (a, b) => (a._1 + b._1, a._2 + b._2)  // Reduce Function
    )
    
    // Compute the average clustering for each vertex
    val averageNeibourhoodClustering: VertexRDD[Double] = totalClusteringScore.mapValues( 
        (id, value) => value match { 
          case (count, clusteringScore) => clusteringScore / count } )
          
    // Create the dataframe for the local centrality graphs and rdd then clear
    val localCentDF: DataFrame = featureCreation(localCentralityRDD, sdf)
    val avgNeibDF: DataFrame  = featureCreation(averageNeibourhoodClustering, sdf)
    clusteringGraph.unpersist(blocking=true)
    
    
    // Calculate the average pagerank score of the each 
    val totalPagerankScore: VertexRDD[(Int, Double)] = pagerankGraph.aggregateMessages[(Int, Double)](        
      triplet => { // Map Function
      // Send the local clustering score and a counter to get the mean
        triplet.sendToDst(1, triplet.srcAttr)
        triplet.sendToSrc(1, triplet.dstAttr)
      },
      // Add the pagerank scores
        (a, b) => (a._1 + b._1, a._2 + b._2)  // Reduce Function
    )
   
    // Compute the average pagerank for each vertex
    val averagePagerankScore: VertexRDD[Double] = totalPagerankScore.mapValues( 
        (id, value) => value match { 
          case (count, clusteringScore) => clusteringScore / count } )
          
    val PageRankDF: DataFrame  = featureCreation(pageRankRDD, sdf)
    val avPageRankDF: DataFrame  = featureCreation(averagePagerankScore, sdf)
    pagerankGraph.unpersist(blocking=true)

    // Compute the final joined vertex feature RDD and return
    val vertexFeatures: DataFrame = PageRankDF.join(avPageRankDF).join(avgNeibDF).join(twoHopDF).join(eigenVecDF).join(localCentDF).join(degreeDF)
    
    return vertexFeatures
  }
  
  def canberraDist(sc: SparkContext, X: RDD[Double], Y: RDD[Double]): Double ={      
    // Create an index based on length for each RDD.
    // Index is added as second value so use map to switch order allowing join to work properly.
    // This can be done in the join step, but added here for clarity.
    val RDDX: RDD[(Long, Double)] = X.zipWithIndex().map(x => (x._2,x._1))
    val RDDY: RDD[(Long, Double)] = Y.zipWithIndex().map(x => (x._2,x._1))
  
    // Join the 2 RDDs on index value. Returns: RDD[(Long, (Double, Double))]
    val RDDJoined: RDD[(Long, (Double, Double))] = RDDX.map(x => (x._1,x._2)).join(RDDY.map(x => (x._1,x._2)))

    // Calculate Canberra Distance   
    val distance: Double = RDDJoined.map{case (id, (x,y)) => { if (x !=0 && y!=0 && !x.isNaN() && !y.isNaN())( math.abs(x - y) / (math.abs(x) + math.abs(y)) ) else(0.0) } }.reduce(_+_)
 
    // Return Value
    return distance
  }

  def createCombinedFeatureVec(g: Graph[Int, Int], sdf: SQLContext, runFull: Boolean, saveFP: Boolean = false): RDD[Double] = {
    
    // Pass the graph to the feature extract methods and save results to disk 
    val gFeatures: DataFrame = extractGlobalFeatures(g, sdf, runFull)
    val vFeatures: DataFrame = extractVertexFeatures(g, sdf)
    val totalFeatures: DataFrame = gFeatures.join(vFeatures)
    
    val totalFeaturesVec: RDD[Double] = totalFeatures.rdd.map {x => x.toSeq.map { y => y.asInstanceOf[Double] }}.flatMap { z => z }

    // Save to disk if passed
    if(saveFP) (totalFeatures.rdd.coalesce(1).saveAsTextFile("GraphFingerPrint.fp") )
    
    return totalFeaturesVec
  }
  
  def createGlobalFeatureVector(g: Graph[Int, Int], sdf: SQLContext, runFull: Boolean, saveFP: Boolean = false): RDD[Double] = {
    
    // Pass the graph to the feature extract methods and save results to disk 
    val gFeatures: DataFrame = extractGlobalFeatures(g, sdf, runFull)
    val gFeaturesVec: RDD[Double] = gFeatures.rdd.map {x => x.toSeq.map { y => y.asInstanceOf[Double] }}.flatMap { z => z }
  
    // Save to disk if passed
    if(saveFP) (gFeatures.rdd.coalesce(1).saveAsTextFile("GraphFingerPrint.fp") )
    
    return gFeaturesVec
  }
  
  def createVertexFeatureVector(g: Graph[Int, Int], sdf: SQLContext, runFull: Boolean, saveFP: Boolean = false): RDD[Double] = {
    
    // Pass the graph to the feature extract methods and save results to disk 
    val vFeatures: DataFrame = extractVertexFeatures(g, sdf)
    val vFeaturesVec: RDD[Double] = vFeatures.rdd.map {x => x.toSeq.map { y => y.asInstanceOf[Double] }}.flatMap { z => z }
  
    // Save to disk if passed
    if(saveFP) (vFeatures.rdd.coalesce(1).saveAsTextFile("GraphFingerPrint.fp") )
    
    return vFeaturesVec
  }
 
  def main(args: Array[String]) {
  
    // Parse CL args including graph locations and run type
    val master = args(0)
    val sourceGraph = args(1)
    val compGraph = args(2)
    val run = args(3)
    val numPart = args(4)
    val jobName = args(5)
    val runFull = args(6)
    
    // Control the log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    println(Calendar.getInstance.getTime)
    val sparkConf: SparkConf = new SparkConf().setAppName(jobName.toString()).setMaster(master)
    val sc:SparkContext = new SparkContext(sparkConf)
    val sqlContext:SQLContext = new org.apache.spark.sql.SQLContext(sc)
    
    // check if only a single fingerprint run is required 
    if(run.toBoolean)
    {
      // Code required to generate a single fingerprint
      println("Generating Single Fingerprint")
      
      // check if the full global features are required
      if(runFull.toBoolean)
      {
        val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, sourceGraph, canonicalOrientation = true, numEdgePartitions = numPart.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
          .partitionBy(PartitionStrategy.RandomVertexCut).groupEdges((a, b) => a)
        
        val graphVec1: RDD[Double] = createCombinedFeatureVec(graph, sqlContext, runFull.toBoolean)
        graph.unpersist(blocking = true)
        println("FingerPrinting Complete")
      }
      else
      {
        val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, sourceGraph, canonicalOrientation = true, numEdgePartitions = numPart.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER).groupEdges((a, b) => a)
        
        val graphVec1: RDD[Double] = createCombinedFeatureVec(graph, sqlContext, runFull.toBoolean)
        graph.unpersist(blocking = true)
        println("FingerPrinting Complete")
      }
    }
    else
    {
      // Code required to compare two graphs
      println("Comparing Two Graphs")
      val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, sourceGraph, canonicalOrientation = true, numEdgePartitions = numPart.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
        .partitionBy(PartitionStrategy.RandomVertexCut, numPart.toInt).groupEdges((a, b) => a)
      
      val graph2: Graph[Int, Int] = GraphLoader.edgeListFile(sc, compGraph, canonicalOrientation = true, numEdgePartitions = numPart.toInt, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER)
        .partitionBy(PartitionStrategy.RandomVertexCut, numPart.toInt).groupEdges((a, b) => a)
     
      // Extract both global and vertex level features
      val graphVec1G: RDD[Double] = createGlobalFeatureVector(graph, sqlContext, runFull.toBoolean).persist(StorageLevel.MEMORY_AND_DISK)
      val graphVec2G: RDD[Double] = createGlobalFeatureVector(graph2, sqlContext, runFull.toBoolean).persist(StorageLevel.MEMORY_AND_DISK)
      val graphVec1V: RDD[Double] = createVertexFeatureVector(graph, sqlContext, runFull.toBoolean).persist(StorageLevel.MEMORY_AND_DISK)
      val graphVec2V: RDD[Double] = createVertexFeatureVector(graph2, sqlContext, runFull.toBoolean).persist(StorageLevel.MEMORY_AND_DISK)
      
      // Calculate the distances and produce final score 
      val globalDist: Double = canberraDist(sc, graphVec1G, graphVec2G)
      val vertexDist: Double = canberraDist(sc, graphVec1V, graphVec2V)
      val totalDist: Double = ((2 * globalDist) + vertexDist)
      
      graphVec1G.unpersist(blocking = true)
      graphVec2G.unpersist(blocking = true)
      graphVec1V.unpersist(blocking = true)
      graphVec2V.unpersist(blocking = true)
      graph.unpersist(blocking = true)
      graph2.unpersist(blocking = true)
      
      println("Total Canberra Distance Between Graphs = " + totalDist)
      println("Comparsion Complete")
    }
   
    // Close program
    println(Calendar.getInstance.getTime)
    sc.stop()
  }
}
