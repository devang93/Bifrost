package DataTransporter

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration
import org.apache.commons.net.ftp.FTPClient
import org.apache.spark.sql.SparkSession

/**
  * Created by Devang Patel on 6/11/18.
  */
object GoogleBigQuerytoADLS {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.gd.project.id", "meta-matrix-90116")
        .config("spark.hadoop.google.cloud.auth.service.accound.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "<path to json file>")
        .appName("googlebigquery_to_adls")
        .getOrCreate()

    val rawDF = spark.read.json("gs://cdap_export/ga_sessions_uo/20180612/test_extract-*")
    print(rawDF.count())
  }
}
