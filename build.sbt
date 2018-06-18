name := "Spark_Data_Transporter"

version := "1.0.2"

scalaVersion := "2.11.8"

unmanagedJars in Compile ++= Seq(
  file("lib/ojdbc7.jar"),
  file("lib/gcs-connector-latest-hadoop2.jar"))

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "com.google.cloud" % "google-cloud-bigquery" % "1.32.0",
  "org.rogach" %% "scallop" % "3.1.2"
)