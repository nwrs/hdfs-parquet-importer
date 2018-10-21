package com.github.nwrs.parquet.importer

import org.rogach.scallop.ScallopConf

class Config(args:Array[String]) extends ScallopConf(args) {
    banner("""|
              |HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]
              |
              |Usage:
              |""".stripMargin)
    val srcFile = opt[String]("srcFile", descr = "CSV file to import.", short = 's', required=true, argName="/path/to/file.csv")
    val destFile = opt[String]("destFile", descr = "Destination Parquet file to export to. Local FS or HDFS paths supported.", short = 'd', required=true, argName="path/file.parquet")
    val schemaFile = opt[String]("schemaFile", descr = "Schema file path.", short = 'f', argName="/path/to/file.schema")
    val dateEnrich = opt[String]("dateEnrich", descr = "Using a date/time string column as a source add additional date/year/month columns to allow smarter partitioning.", short = 'e', argName="date_time_col")
    val partitionCols = opt[String]("partitionCols", descr = "Partition columns.", short = 'p', argName="column,column,...")
    val sortCols = opt[String]("sortCols", descr = "Sort columns.", short = 'o', argName="column,column,...")
    val slashEscapes = opt[Boolean]("slashEscapes", descr = """Use '\"' as an escape character instead of '""' to denote quotes within a quote.""", short = 'q')
    val delimiter = opt[String]("delimeter", descr = "CSV delimiter character, default is ','.", short = 'l', default = Some(","), argName="char")
    val sparkThreads = opt[String]("threads", descr = "Number of Spark threads, default is # processors.", short = 't', default = Some("*"), argName="n")
    val twitterCleanse = opt[Boolean]("twitterCleanse", descr = "Remove corrupted rows in Twitter sourced CSV files.", short = 'w', default = Some(false))
    helpWidth(120)
    verify()
}
