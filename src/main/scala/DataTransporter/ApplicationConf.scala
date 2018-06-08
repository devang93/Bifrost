package DataTransporter

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
  * Created by Devang Patel on 6/1/2018.
  */
class ApplicationConf(args: Seq[String]) extends ScallopConf(args) {

  val ingestionType = opt[String](name = "ingestionType", noshort = false, required = true, descr = "Ingestion type")
  // Oracle Parameters
  val user = opt[String](name = "user", noshort = false, required = true, descr = "Oracle user name")
  val password = opt[String](name = "password", noshort = false, required = true, descr = "Oracle user password")
  val hostport = opt[String](name = "hostport", noshort = false, required = true, descr = "Oracle database host:port")
  val db = opt[String](name = "db", noshort = false, required = true, descr = "Oracle logical database")
  val sqlQuery = opt[String](name = "sqlQuery", noshort = false, required = true, descr = "Oracle SQL query to pull data")

  // S3 Parameters
  val s3path = opt[String](name="s3path", noshort = false, required = true, descr = "s3 input file path")
  val s3accessKey = opt[String](name="s3accessKey", noshort = false, required = true, descr = "s3 access key")
  val s3secretKey = opt[String](name="s3secretKey", noshort = false, required = true, descr = "s3 secret key")
  val inputFormat = opt[String](name="inputFormat", noshort = false, required = true, descr = "input file format")
  val outputFormat = opt[String](name="outputFormat", noshort = false, required = true, descr = "output file format")
  val inputOptions = opt[String](name="inputOptions", noshort = false, required = false, descr = "input options for reading data")
  val outputOptions = opt[String](name="outputOptions", noshort = false, required = false, descr = "output options for writing data")

  // ADLS Parameters
  val adls = opt[String](name = "adls", noshort = false, required = true, descr = "Azure Data Lake store uri")
  val adlsOutputPath = opt[String](name = "adlsOutputPath", noshort = false, required = true, descr = "ADLS output path")
  val tenantId = opt[String](name = "tenantId", noshort = false, required = true, descr = "Azure tenantId")
  val spnClientId = opt[String](name = "spnClientId", noshort = false, required = true, descr = "Azure service principle ApplicationId")
  val spnClientSecret = opt[String](name = "spnClientSecret", noshort = false, required = true, descr = "Azure service principle secret")
  val writeMode = opt[String](name = "writeMode", noshort = false, required = false, default = Some("overwrite"), descr = "Writemode at output location")
  val numPartitions = opt[Int](name = "numPartitions", noshort = false, required = true, descr = "Number of partitions for output data")
  val job_run_id = opt[String](name = "job_run_id", noshort = false, required = false, descr = "Optional job execution id")
  val execution_date = opt[String](name = "execution_date", noshort = false, required = false, descr = "Optional execution date")
  val prev_exec_date = opt[String](name = "prev_exec_date", noshort = false, required = false, descr = "Optional previous execution date")

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
      case _ => Left("Unknown Ingestion type: "+ingestionType)
    }
  }

  verify()

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

}

