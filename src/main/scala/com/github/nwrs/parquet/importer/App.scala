package com.github.nwrs.parquet.importer

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object App {
  val log:Logger = LoggerFactory.getLogger(classOf[App])

  def main(args : Array[String]) {

    log.info("HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]")

    val conf = new Config(args)

    val builder = SparkSession.builder
      .appName("CSV -> Parquet Importer")
      .config("spark.debug.maxToStringFields","100")

    if (conf.sparkOpt.isDefined) {
      conf.sparkOpt().split(",").map(_.trim).foreach { o =>
        val optPair = o.split("=").map(_.trim)
        builder.config(optPair(0),optPair(1))
      }
    }

    implicit val ss = builder.master(s"local[${conf.sparkThreads()}]").getOrCreate()

    val start = System.currentTimeMillis()
    val df = readCSVWriteParquet(conf)

    log.info(s"Complete in ${TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start)}s")
    log.info(s"Schema:\n${df.schema.treeString}")

  }

}
