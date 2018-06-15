package DataTransporter

import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 3/24/2018.
  */

object Main {

  private val log = LoggerFactory.getLogger("DataTransporter.Main")

  def main(args: Array[String]): Unit = {

    val appConf = new ApplicationConf(args)

    val readOptionsMap = {
    appConf.inputOptions.toOption match {
        case Some(options) => {
          options.substring(1, options.length-1).split(",").map(_.split(":")).map {
            case Array(k,v) => (k, v)
          }.toMap
        }
        case None => println("No input options defined!"); Map()
      }
    }

    val writeOptionsMap = {
      appConf.outputOptions.toOption match {
        case Some(options) => {
          options.substring(1, options.length-1).split(",").map(_.split(":")).map {
            case Array(k,v) => (k, v)
          }.toMap
        }
        case None => println("No output options defined!"); Map()
      }
    }

    println(readOptionsMap)
    println(writeOptionsMap)

    appConf.ingestionType() match {
      case "ORACLE_TO_ADLS" => OracleToADLS.execute(appConf)
      case "S3_TO_ADLS" => println("S3 to ADLS is invoked")//S3toADLS.execute(appConf)
      case "GC_TO_ADLS" => GoogleBigQuerytoADLS.execute(appConf)
    }
  }
}
