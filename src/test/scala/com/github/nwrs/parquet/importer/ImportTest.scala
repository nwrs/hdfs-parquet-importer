package com.github.nwrs.parquet.importer

import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ImporterTests extends FunSuite with Matchers with BeforeAndAfterAll {
  val tmpDir = "/tmp"
  val destFile = s"$tmpDir/test.parquet"
  implicit val ss = SparkSession.builder.master("local[*]").config("spark.sql.shuffle.partitions",1).getOrCreate()
  var df: Option[DataFrame] = None

  def delteFileIfPresent()= {
    val oldFile = new File(destFile)
    if (oldFile.exists) Runtime.getRuntime().exec(s"rm -rf $oldFile")
  }

  override def beforeAll() = {
    ss.sparkContext.setLogLevel("WARN")
    delteFileIfPresent()
    val args = Array("--srcFile", "src/test/data/test-tweets.csv",
      "--schemaFile", "src/test/data/tweets.schema",
      "--destFile", destFile,
      "--dateEnrich","tweet_time",
      "--partitionCols", "year,month",
      "--twitterCleanse")

    val conf = new Config(args)
    df = Some(readCSVWriteParquet(conf))
    df.get.show(truncate = false)
  }

  override def afterAll(): Unit = delteFileIfPresent()

  test("File is created") {
    df.isDefined should be(true)
    val f = new File(destFile)
    f.exists should be(true)
  }

  test("Expected number of rows created") {
    df.get.count should equal (10)
  }

  test("Expected number of columns created") {
    df.get.schema.length should equal (34)
  }

  test("Date enrichment adds extra columns") {
    noException should be thrownBy df.get.col("date")
    noException should be thrownBy df.get.col("year")
    noException should be thrownBy df.get.col("month")
  }

  test("Twitter row cleanse removes corrupt rows") {
    df.get.filter(r => r.getAs[String]("tweetid")==null).collect().length should equal(0)
  }

  test("Parquet file layout and partitioning as expected") {
    val expectedFilePartitions = Map(("year=2014" -> Seq("month=07","month=11")),
                                     ("year=2015" -> Seq("month=02","month=03","month=05","month=11")),
                                     ("year=2016" -> Seq("month=04")),
                                     ("year=2017" -> Seq("month=02","month=03")))
    val dir = new File(destFile)
    dir.isDirectory should equal (true)
    val years = dir.listFiles().filter(_.isDirectory).toSet

    years.size should equal (expectedFilePartitions.size)

    years.foreach { y =>
      expectedFilePartitions.get(y.getName).isDefined should equal (true)
      y.listFiles().map(_.getName).toSet.intersect(Set(expectedFilePartitions.get(y.getName).get :_*)).size should equal (y.listFiles.size)
    }
  }


}
