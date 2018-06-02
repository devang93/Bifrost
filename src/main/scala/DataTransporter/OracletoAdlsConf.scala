package DataTransporter

import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.Help

/**
  * Created by devang on 6/1/2018.
  */
class OracletoAdlsConf(args: Seq[String]) extends ScallopConf(args){

  val user = opt[String](name = "user", noshort = false, required = true, descr = "Oracle user name")
  val password = opt[String](name = "password", noshort = false, required = true, descr = "Oracle user password")
  val hostport = opt[String](name = "hostport", noshort = false, required = true, descr = "Oracle database host:port")
  val db = opt[String](name = "db", noshort = false, required = true, descr = "Oracle logical database")
  val adlsOutputPath = opt[String](name = "adlsOutputPath", noshort = false, required = true, descr = "ADLS output path")
  val sqlQuery = opt[String](name = "sqlQuery", noshort = false, required = true, descr = "Oracle SQL query to pull data")
  val adls = opt[String](name = "adls", noshort = false, required = true, descr = "Azure Data Lake store uri")
  val tenantId = opt[String](name = "tenantId", noshort = false, required = true, descr = "Azure tenantId")
  val spnClientId = opt[String](name = "spnClientId", noshort = false, required = true, descr = "Azure service principle ApplicationId")
  val spnClientSecret = opt[String](name = "spnClientSecret", noshort = false, required = true, descr = "Azure service principle secret")
  val writeMode = opt[String](name = "writeMode", noshort = false, required = false, default = Some("overwrite"), descr = "Writemode at output location")
  val job_run_id = opt[String](name = "job_run_id", noshort = false, required = false, descr = "Optional job execution id")
  val execution_date = opt[String](name = "execution_date", noshort = false, required = false, descr = "Optional execution date")
  val prev_exec_date = opt[String](name = "prev_exec_date", noshort = false, required = false, descr = "Optional previous execution date")

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
  }

  verify()

}
