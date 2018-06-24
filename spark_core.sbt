name := "Spark-Core-Project"
 
version := "1.0"
 
scalaVersion := "2.11.6"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

libraryDependencies += "it.unimi.dsi" % "fastutil" % "7.0.7"

libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "9.4.11.v20180605"

