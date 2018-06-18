package DataTransporter

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
  * Created by Devang Patel on 6/1/2018.
  */
class ApplicationConf(args: Seq[String]) extends ScallopConf(args) {

  val ingestionType = opt[String](name = "ingestionType", noshort = false, required = true, descr = "Ingestion type")
  val inputFormat = opt[String](name="inputFormat", noshort = false, descr = "input file format")
  val outputFormat = opt[String](name="outputFormat", noshort = false, descr = "output file format")
  val inputOptions = opt[String](name="inputOptions", noshort = false, descr = "input options for reading data")
  val outputOptions = opt[String](name="outputOptions", noshort = false, descr = "output options for writing data")

  // Oracle Parameters
  val user = opt[String](name = "user", noshort = false, descr = "Oracle user name")
  val password = opt[String](name = "password", noshort = false, descr = "Oracle user password")
  val hostport = opt[String](name = "hostport", noshort = false, descr = "Oracle database host:port")
  val db = opt[String](name = "db", noshort = false, descr = "Oracle logical database")
  val sqlQuery = opt[String](name = "sqlQuery", noshort = false, descr = "Oracle SQL query to pull data")

  // S3 Parameters
  val s3path = opt[String](name="s3path", noshort = false, descr = "s3 input file path")
  val s3accessKey = opt[String](name="s3accessKey", noshort = false, descr = "s3 access key")
  val s3secretKey = opt[String](name="s3secretKey", noshort = false, descr = "s3 secret key")

  // Google BigQuery Parameters
  val gcprojectId = opt[String](name="gcprojectId", noshort = false, descr = "google cloud project id")
  val bqdatasetName = opt[String](name="bqdatasetName", noshort = false, descr = "google bigquert dataset name")
  val bqtableName = opt[String](name="bqtableName", noshort = false, descr = "google bigquery table to pull data")
  val gcpath = opt[String](name="gcpath", noshort = false, descr = "google cloud path to extract data")
  val extractCompression = opt[String](name="extractCompression", noshort = false, descr = "compression for extracted files")
  val extractFormat = opt[String](name="extractFormat", noshort = false, descr = "format for extracted data")
  val googleCredFile = opt[String](name="googleCredFile", noshort = false, descr = "google credentials json file")

  // ADLS Parameters
  val adls = opt[String](name = "adls", noshort = false, descr = "Azure Data Lake store uri")
  val adlsOutputPath = opt[String](name = "adlsOutputPath", noshort = false, descr = "ADLS output path")
  val tenantId = opt[String](name = "tenantId", noshort = false, descr = "Azure tenantId")
  val spnClientId = opt[String](name = "spnClientId", noshort = false, descr = "Azure service principle ApplicationId")
  val spnClientSecret = opt[String](name = "spnClientSecret", noshort = false, descr = "Azure service principle secret")
  val writeMode = opt[String](name = "writeMode", noshort = false, default = Some("overwrite"), descr = "Writemode at output location")
  val numPartitions = opt[Int](name = "numPartitions", noshort = false, descr = "Number of partitions for output data")
  val job_run_id = opt[String](name = "job_run_id", noshort = false, descr = "Optional job execution id")
  val execution_date = opt[String](name = "execution_date", noshort = false, descr = "Optional execution date")
  val prev_exec_date = opt[String](name = "prev_exec_date", noshort = false, descr = "Optional previous execution date")

  // Validation of arguments.
  validate (ingestionType) { (ingestion) =>
    ingestion match {
      case "ORACLE_TO_ADLS" => {
        println("Data ingestion Requested from ORACLE to ADLS.")
        codependent(user, password, hostport, db, sqlQuery)
        println("All ORACLE parameters are defined.")
        codependent(adls, adlsOutputPath, tenantId, spnClientId, spnClientSecret)
        println("All ADLS parameters are defined")
        Right(Unit)
      }
      case "S3_TO_ADLS" => {
        println("Data Ingestion Requested from S3 to ADLS.")
        codependent(s3path, s3accessKey, s3secretKey, inputFormat, outputFormat, inputOptions)
        println("All S3 parameters are defined")
        codependent(adls, adlsOutputPath, tenantId, spnClientId, spnClientSecret)
        println("All ADLS parameters are defined")
        Right(Unit)
      }
      case "GC_TO_ADLS" => {
        println("Data Ingestion Requested from Google Cloud to ADLS.")
        codependent(gcprojectId, bqdatasetName, bqtableName, gcpath, extractCompression, extractFormat, googleCredFile)
        println("All Google Cloud parameters are defined")
//        codependent(adls, adlsOutputPath, tenantId, spnClientId, spnClientSecret)
        println("All ADLS parameters are defined")
        Right(Unit)
      }
      case _ => Left("Unknown Ingestion type: "+ingestionType)
    }
  }

  verify()

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

}

