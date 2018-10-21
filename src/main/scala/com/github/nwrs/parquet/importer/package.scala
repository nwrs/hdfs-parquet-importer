package com.github.nwrs.parquet

import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

package object importer {

  lazy val log:Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Populate a spark schema using a config file of the format columnName=Type
    * Order of fields in config file must strictly match CSV file.
    * @param file Path to schema file
    * @return StructType schema
    */
  def createSchema(file:String):StructType = {
    val fields = Source.fromFile(file)
      .getLines()
      .filter(!_.startsWith("#"))
      .map(_.split("="))
      .map( s => (s(0).trim,s(1).trim))
      .map( e => e._2 match {
        case "String" => new ColumnName(e._1).string
        case "Long" => new ColumnName(e._1).long
        case "Int" => new ColumnName(e._1).int
        case "Double" => new ColumnName(e._1).double
        case "Float" => new ColumnName(e._1).float
        case "Boolean" => new ColumnName(e._1).boolean
        // Allow match error for unsupported types
        // TODO more types as required ?
      })
    StructType(fields.toSeq)
  }

  /**
    * Enriches a dataframe with date, year and column fields to allow for smarter Parquet partitioning
    * @param dateTimeCol A date time string of format "yyyy-mm-dd hh:mm"
    * @param df Dataframe
    * @param sc Implicit spark session
    * @return enriched dataframe
    */
  def dateEnrichFromDateTimeStr(dateTimeCol:String, df:DataFrame)(implicit sc:SparkSession):DataFrame= {
    val dateTimeRegEx = """([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})""".r
    sc.sqlContext.udf.register("extract_date", (dateTime: String) => if (dateTime !=null) dateTime.split(" ")(0) else "")
    sc.sqlContext.udf.register("extract_year", (dateTime: String) => {
      if (dateTime !=null)
        dateTime match {
          case dateTimeRegEx(year, _*) => year
          case _ => ""
        }
      else
        ""
    })
    sc.sqlContext.udf.register("extract_month", (dateTime: String) => {
      if (dateTime !=null)
        dateTime match {
          case dateTimeRegEx(y, month, _*) => month
          case _ => ""
        }
      else
        ""
    })
    df.withColumn("date", callUDF("extract_date", col(dateTimeCol)))
      .withColumn("year", callUDF("extract_year", col(dateTimeCol)))
      .withColumn("month", callUDF("extract_month", col(dateTimeCol)))
  }

  /**
    * Filter out suspect rows, a workaround for occasional (~ 1/100000) corrupt rows in twitter datasets that can break the parquet export
    * Issues may be be related to double quote escaping at the start of non-latin character-set tweets?
    * N.B. Only usable for datasets using the specific Twitter schema!
    * @param df Dataframe to cleanse
    * @param sc Implicit Spark session
    * @return Cleansed dataframe
    */
  def filterOutSuspectTwitterRows(df: DataFrame)(implicit sc:SparkSession):DataFrame = {
    val pattern = """([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})""".r
    import sc.implicits._
    val badTweetIds = df.select("tweetid", "tweet_time").map { r =>
      r.getAs[String](1) match {
        case pattern(year, month, day, hour, minutes) => None
        case _ => Some(r.getLong(0))
      }
    }.filter(_.isDefined).map(_.get).collect
    if (badTweetIds.length > 0)
      df.filter("tweetid NOT IN " + badTweetIds.mkString("(", ",", ")"))
    else
      df
  }


  /**
    * Append a new column of type Array[String] sourced from a string column containing data in the format "[element1, element2, element3]"
    * @param srcCol The column name containing a string formatted as "[text, text, text]"
    * @param df Dataframe
    * @param sc SparkSession
    * @return Dataframe with added column
    */
  def parseAndAppendArrayCol(srcCol:String, df:DataFrame, removeSrc:Boolean)(implicit sc:SparkSession):DataFrame= {
    sc.sqlContext.udf.register("expand_array", (arrayStr: String) => if (arrayStr!=null && arrayStr.nonEmpty) arrayStr.substring(1,arrayStr.length-1).split(",").map(_.trim) else Array[String]())
    df.withColumn(srcCol+"_array", callUDF("expand_array", col(srcCol)))
  }

  def readCSVWriteParquet(conf: Config)(implicit ss:SparkSession):DataFrame = {
    // create csv reader
    val reader = ss.read
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .option("charset", "UTF8")
      .option("delimiter", conf.delimiter())
      .option("escape", if (conf.slashEscapes.isDefined) "\\" else "\"")

    // populate schema from config file if configured
    if (conf.schemaFile.isDefined) {
      log.info(s"Schema is '${conf.schemaFile()}'")
      reader.schema(createSchema(conf.schemaFile()))
    } else {
      log.info("Inferring schema")
    }

    // read file
    log.info(s"Reading '${conf.srcFile()}'")
    val df = reader.csv(conf.srcFile())

    // Cleanse any corrupted rows that can occur in some Twitter sourced datasets
    val cleansed = if (conf.twitterCleanse()) {
      log.info("Cleansing corrupted rows")
      filterOutSuspectTwitterRows(df)
    } else
      df

    // enrich with expanded date fields
    val enriched = if(conf.dateEnrich.isDefined) {
      log.info(s"Enriching with date columns from '${conf.dateEnrich()}'")
      dateEnrichFromDateTimeStr(conf.dateEnrich(), cleansed)
    } else
      cleansed

    // sort
    val sorted = if (conf.sortCols.isDefined) {
      val sortCols = conf.sortCols().split(",").map(_.trim).map(enriched(_))
      log.info(s"Sorting by ${sortCols.mkString(", ")}")
      enriched.sort(sortCols :_*)
    } else
      enriched

    // partition and write
    if(conf.partitionCols.isDefined) {
      val partCols = conf.partitionCols().split(",").map(_.trim)
      log.info(s"Partitioning by ${partCols.mkString(", ")}")
      val partitioned = sorted.repartition(partCols.map(sorted(_)) :_*)
      log.info(s"Writing '${conf.destFile()}'")
      partitioned.write.partitionBy(partCols :_*).parquet(conf.destFile())
      partitioned
    } else {
      log.info(s"Writing '${conf.destFile()}'")
      sorted.write.parquet(conf.destFile())
      sorted
    }
  }

}
