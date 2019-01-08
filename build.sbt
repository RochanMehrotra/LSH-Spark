name := "lshSpark"

version := "0.1"

scalaVersion := "2.11.11"


libraryDependencies += "org.apache.opennlp" % "opennlp-tools" % "1.8.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
