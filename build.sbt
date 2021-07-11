name := "Project_GCP_Spark_Scala"

version := "0.1"

scalaVersion := "2.11.11"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
)
