package com.github.nwrs.parquet

import org.apache.spark.sql.{ColumnName, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{callUDF, col}
import org.apache.spark.sql.types.StructType

import scala.io.Source

package object importer {

  def createSchema(file:String):StructType = {
    val fields = Source.fromFile(file)
      .getLines()
      .filter(!_.startsWith("#"))
      .map(_.split("="))
      .map( s => (s(0).trim,s(1).trim))
      .map( e => e._2 match {
        case "String" => new ColumnName(s"${e._1}").string
        case "Long" => new ColumnName(s"${e._1}").long
        case "Double" => new ColumnName(s"${e._1}").double
        case "Boolean" => new ColumnName(s"${e._1}").boolean
        // TODO more types as required...
      })
    StructType(fields.toSeq)
  }

  // TODO scala doc and comment me please and hide in a utility class :-)
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

  // filter out suspect rows - weird hack to get around rare problem in twitter datasets, only used manually
  def filterOutSuspectRows(df: DataFrame)(implicit sc:SparkSession):DataFrame = {
    val pattern = """([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2})""".r
    import sc.implicits._
    val badTweetIds = df.select("tweetid", "tweet_time").map { r =>
      r.getAs[String](1) match {
        case pattern(year, month, day, hour, minutes) => None
        case _ => Some(r.getLong(0))
      }
    }.filter(_.isDefined).map(_.get).collect
    df.filter("tweetid NOT IN " + badTweetIds.mkString("(", ",", ")"))
  }


}
