package DataTransporter

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 6/1/2018.
  */
object S3toADLS {

    private val log = LoggerFactory.getLogger("S3toAdls")

    def execute(conf: ApplicationConf): Unit = {

        val spark = SparkSession.builder()
                    .config("spark.hadoop.s3a.access.key", conf.s3accessKey())
                    .config("spark.hadoop.s3a.secret.key", conf.s3secretKey())
                    .config("spark.hadoop.dfs.adls.oauth2.client.id", conf.spnClientId())
                    .config("spark.hadoop.dfs.adls.oauth2.credential", conf.spnClientSecret())
                    .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
                    .config("spark.hadoop.dfs.adls.oauth2.refresh.url", s"https://login.microsoftonline.com/${conf.tenantId()}/oauth2/token")
                    .appName("s3_to_adls_transfer")
                    .getOrCreate()

        val inputDF = spark.read.format("csv")
                    .option("header", "false")
                    .option("inferSchema", "false")
                    .option("delimiter", "|")
                    .load(conf.s3path())

        inputDF.write.format("csv")
              .option("delimiter", "|").option("compression", "gzip")
              .save(conf.adls()+conf.adlsOutputPath())

        }

}

