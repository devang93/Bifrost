name := "Spark_Data_Transporter"

version := "1.0.2"

scalaVersion := "2.11.8"

unmanagedJars in Compile ++= Seq(
  file("lib/ojdbc7.jar"))

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.2-hadoop2" exclude ("com.google.guava", "guava-jdk5"),
  "org.rogach" %% "scallop" % "3.1.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}