package DataTransporter

import org.apache.spark.sql.SparkSession
import java.io.FileInputStream
import java.util.UUID

import com.google.cloud.bigquery._
import com.google.auth.oauth2.ServiceAccountCredentials
import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 6/11/18.
  */
object GoogleBigQuerytoADLS {

  private val log = LoggerFactory.getLogger("GoogleBigQueryToAdls")

//  def main(args: Array[String]): Unit = {
//    val credentialsPath = "google_cred.json"
//    var credentials: ServiceAccountCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
//    val bg = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId("meta-matrix-90116").build().getService();
//    val table = "ga_sessions_20180611"
//    val dataSetName = "127577860"
//    val uri = "gs://cdap_export/test_ga_sessions_uo/20180613/test_extract_dpatel-*.json.gz"
//    val tableId: TableId = TableId.of(dataSetName, table)
//
//    val extractJobConfiguration = ExtractJobConfiguration.newBuilder(tableId, uri).setCompression("GZIP").setFormat("NEWLINE_DELIMITED_JSON").build()
//
//    val jobInfo: JobInfo = JobInfo.newBuilder(extractJobConfiguration).setJobId(JobId.of(UUID.randomUUID().toString)).build()
//
//    var job: Job = null
//    try {
//      job = bg.create(jobInfo)
//      println(job.getJobId.getJob)
//      println("Job Status: "+ job.getStatus.getState)
//      val j = job.waitFor()
//      println(j.getStatus.getState)
//    } catch {
//      case e: BigQueryException => println("JOB failed")
//      case e: Exception => println("Other Exception!"); e.printStackTrace()
//    }
//
//    val spark = SparkSession.builder()
//      .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
//      .config("fs.gd.project.id", "meta-matrix-90116")
//      .config("google.cloud.auth.service.accound.enable", "true")
//      .config("google.cloud.auth.service.account.json.keyfile", "google_cred.json")
//      .appName("googlecloud_to_adls")
//        .master("yarn")
//      .getOrCreate()
//
//    val raw = spark.read.json("gs://cdap_export/test_ga_sessions_uo/20180613/test_extract_dpatel-*.json.gz")
//  }

  def execute(conf: ApplicationConf): Unit = {

    val table = conf.bqtableName()
    val dataSetName = conf.bqdatasetName()
    val uri = conf.gcpath()+"/test_extract-*.json.gz"

    // Run the Google BigQuery Job to export the data.
    val credentialsPath = conf.googleCredFile()
    var credentials: ServiceAccountCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
    val bg = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(conf.gcprojectId()).build().getService();
    val tableId: TableId = TableId.of(dataSetName, table)
    val extractJobConfiguration = ExtractJobConfiguration.newBuilder(tableId, uri)
                                      .setCompression(conf.extractCompression())
                                      .setFormat(conf.extractFormat())
                                      .build()
    val jobInfo: JobInfo = JobInfo.newBuilder(extractJobConfiguration).setJobId(JobId.of(UUID.randomUUID().toString)).build()
    var job: Job = null
    try {
        job = bg.create(jobInfo)
        log.info(job.getJobId.getJob)
        log.info(s"Job ${job.getJobId} Status: ${job.getStatus.getState}")
        job = job.waitFor()
        log.info(s"Job ${job.getJobId} Status: ${job.getStatus.getState}")
      } catch {
        case e: BigQueryException => log.error("Big Query Job failed with Exception:"); e.printStackTrace()
        case e: Exception => log.error("Exception occured while doing extract job!"); e.printStackTrace()
      }

    val spark = SparkSession.builder()
      .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("fs.gd.project.id", conf.gcprojectId())
      .config("google.cloud.auth.service.accound.enable", "true")
      .config("google.cloud.auth.service.account.json.keyfile", "google_cred.json")
      .appName("googlecloud_to_adls")
      .getOrCreate()

    val raw = spark.read.json(conf.gcpath())
    println(raw.count())
  }
}
