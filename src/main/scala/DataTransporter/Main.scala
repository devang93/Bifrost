package DataTransporter

import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 3/24/2018.
  */

object Main {

  private val log = LoggerFactory.getLogger("DataTransporter.Main")

  def main(args: Array[String]): Unit = {

    val appConf = new ApplicationConf(args)

    appConf.ingestionType() match {
      case "ORACLE_TO_ADLS" => OracleToADLS.execute(appConf)
      case "S3_TO_ADLS" => S3toADLS.execute(appConf)
    }
  }
}
