package com.github.nwrs.parquet.importer

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{callUDF, col}
import org.rogach.scallop._

object App {

  def parseAndAppendArrayCol[T](srcCol:String, df:DataFrame)(implicit sc:SparkSession):DataFrame= {
    sc.sqlContext.udf.register("expand_array", (arrayStr: String) => if (arrayStr!=null && arrayStr.nonEmpty) arrayStr.substring(1,arrayStr.length-1).split(",").map(_.trim) else Array[String]())
    df.withColumn(srcCol+"_array", callUDF("expand_array", col(srcCol)))
  }

  def main(args : Array[String]) {
    val opts = new ScallopConf(args) {
      banner("""|
                |HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]
                |
                |For usage see below:""".stripMargin)
      val srcFile = opt[String]("srcFile", descr = "CSV file to import", short = 's', required=true, argName="/path/to/file.csv")
      val destFile = opt[String]("destFile", descr = "Destination Parquet file to export to. Local FS or HDFS paths supported", short = 'd', required=true)
      val schemaFile = opt[String]("schemaFile", descr = "Schema file", short = 'f', argName="/path/to/file.schema")
      val dateEnrich = opt[String]("dateEnrich", descr = "Enrich string formatted date/time col to a date/year/month columns to allow smarter partitioning", short = 'e', argName="date_time src col")
      val partitionCols = opt[String]("partitionCols", descr = "Partition columns", short = 'p', argName="column,column,...")
      val sortCols = opt[String]("sortCols", descr = "Sort columns", short = 'o', argName="column")
      val slashEscapes = opt[Boolean]("slashEscapes", descr = """Use '\"' as an escape character instead of '""' to denote quotes within a quote""", short = 'q')
      val delimeter = opt[String]("delimeter", descr = "CSV delimeter character, default is ','", short = 'l', default = Some(","))
      val sparkThreads = opt[String]("sparkThreads", descr = "Numbner of Spark threads, default is # processors", short = 't', default = Some("*"))
      val twitterCleanse = opt[Boolean]("twitterCleanse", descr = "Remove corrupted rows in Twitter sourced CSV files", short = 'w')
      verify()
    }

    implicit val sc = SparkSession.builder.master(s"local[${opts.sparkThreads()}]").getOrCreate()

    // create csv reader
    val reader = sc.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("charset", "UTF8")
      .option("delimiter", opts.delimeter())
      .option("escape", if (opts.slashEscapes.isDefined) "\\" else "\"")

    // populate schema from config file if configured
    if (opts.schemaFile.isDefined) reader.schema(createSchema(opts.schemaFile()))

    // read file
    val df = reader.csv(opts.srcFile())

    // Cleanse any corrupted rows that can occur in some Twitter sourced datasets
    val cleansed = if (opts.twitterCleanse.isDefined)
      filterOutSuspectTwitterRows(df)
    else
      df

    // enrich with expanded date fields if required
    val enriched = if(opts.dateEnrich.isDefined)
      dateEnrichFromDateTimeStr(opts.dateEnrich(), cleansed)
    else
      cleansed

    // sort as required
    val sorted = if (opts.sortCols.isDefined) {
      enriched.sort(opts.sortCols().split(",").map(_.trim).map(enriched(_)) :_*)
    } else
      enriched

    // partition as required
    val partitioned = if(opts.partitionCols.isDefined) {
      val partCols = opts.partitionCols().split(",").map(_.trim)
      sorted.repartition(partCols.map(sorted(_)) :_*).write.partitionBy(partCols :_*)
    } else
      sorted.write

    // write
    partitioned.parquet(opts.destFile())
    sorted.printSchema()

    // apply arrays
    //parseAndAppendArrayCol("hashtags", enriched)

  }

}
