name := "Graph Fingerprint Comparison"
 
version := "1.0"
 
scalaVersion := "2.10.5"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"

libraryDependencies += "graphframes" % "graphframes" % "0.1.0-spark1.6"

libraryDependencies += "ml.sparkling" %% "sparkling-graph-examples" % "0.0.5"

libraryDependencies += "ml.sparkling" %% "sparkling-graph-loaders" % "0.0.5"

libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.5"
