package DataTransporter

import java.nio.file.{Files, Paths}
import java.util.Properties
import org.apache.spark.internal.config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 6/1/2018.
  */
object OracleToADLS {

    private val log = LoggerFactory.getLogger("OracleSparkTransfer.Main")

    // function to get lower and upper bound.
    def getBounds(spark: SparkSession, jdbcUrl: String, queryString: String, oracleProperties: Properties) = {
      log.info("Getting upper and lower bound for the data to be transferred...")
      val query = s"SELECT min(ROWNUM), max(ROWNUM) FROM (${queryString})"
      val bounds = spark.read.jdbc(
        url = jdbcUrl,
        table = s"(${query}) oracle_data_count",
        properties = oracleProperties).take(1)
      (bounds(0)(0).toString.toDouble.toLong, bounds(0)(1).toString.toDouble.toLong)
    }

    def substituteExecutionParams(input: String, config: ApplicationConf): String = {
      var output = new String(input)

      config.job_run_id.toOption match {
        case Some(id) =>
          val reg = """\$job_run_id""".r
          output = reg.replaceAllIn(output, id.toString)
        case None => log.warn("No job_run_id value provided at runtime!")
      }
      config.execution_date.toOption match {
        case Some(exec_date) =>
          val reg = """\$execution_date""".r
          output = reg.replaceAllIn(output, exec_date)
        case None => log.warn("No execution_date value provided at runtime!")
      }
      config.prev_exec_date.toOption match {
        case Some(prev_exec_date) =>
          val reg = """\$prev_execution_date""".r
          output = reg.replaceAllIn(output, prev_exec_date)
        case None => log.warn("No prev_execution_date value provided at runtime!")
      }
      output
    }

    def execute(conf : ApplicationConf): Unit = {

      val spark = SparkSession.builder()
                  .config("spark.hadoop.dfs.adls.oauth2.client.id", conf.spnClientId())
                  .config("spark.hadoop.dfs.adls.oauth2.credential", conf.spnClientSecret())
                  .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
                  .config("spark.hadoop.dfs.adls.oauth2.refresh.url", s"https://login.microsoftonline.com/${conf.tenantId}/oauth2/token")
                  .appName("Spark_Oracle_Data_Transfer").getOrCreate()

          val jdbcURL = s"jdbc:oracle:thin:@//${conf.hostport()}/${conf.db()}"
          val oracleProperties = new Properties()
          oracleProperties.setProperty("user", conf.user())
          oracleProperties.setProperty("password", conf.password())
          oracleProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
          oracleProperties.setProperty("fetchsize", "2000")
          val queryString = substituteExecutionParams(new String(Files.readAllBytes(Paths.get(conf.sqlQuery())), "UTF-8"), conf)
          log.info("Oracle Query to pull data: ")
          log.info(queryString)
          val bounds  = getBounds(spark, jdbcURL, queryString, oracleProperties)

          val oracleDF = spark.read.jdbc(url = jdbcURL,
            table = s"(SELECT ROWNUM as NUM_RECORDS, t.* FROM (${queryString}) t ) oracle_data_pull",
            columnName = "num_records",
            lowerBound = bounds._1,
            upperBound = bounds._2,
            numPartitions = conf.numPartitions(),
            connectionProperties = oracleProperties)

          // drop num_records column.
          val outputDF = oracleDF.drop("num_records")
          outputDF.printSchema()
          // write data out as parquet files.
          val outputPath = substituteExecutionParams(conf.adls()+conf.adlsOutputPath(), conf)
          log.info("writing data from Oracle Source to Sink : "+outputPath)
          outputDF.write.mode(conf.writeMode()).parquet(outputPath)

      }
}
