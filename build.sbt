name := "Logs"

version := "1.0"

scalaVersion := "2.10.6"

jarName in assembly := "Logs.jar"

libraryDependencies  += "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided"
