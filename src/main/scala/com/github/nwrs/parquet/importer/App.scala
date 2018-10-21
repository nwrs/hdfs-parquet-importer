package com.github.nwrs.parquet.importer

import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object App {
  val log:Logger = LoggerFactory.getLogger(classOf[App])

  def main(args : Array[String]) {
    log.info("HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]")
    val conf = new Config(args)
    implicit val ss = SparkSession.builder
      .appName("CSV -> Parquet Importer")
      .config("spark.debug.maxToStringFields","100")
      .master(s"local[${conf.sparkThreads()}]").getOrCreate()
    val df = readCSVWriteParquet(conf)
    log.info("Done")
  }

}
