## HDFS Parquet Importer

A simple standalone Spark application to convert CSV files to Parquet format and import into HDFS.

* Converts from CSV to Parquet format.
* Supports either schema inference or the supply of an explicitly typed schema.
* Optionally sorts and re-partitions by multiple columns to optimise for filtering and read performance.
* Optionally adds enriched/reformatted date fields to allow for smarter date based partitioning.
* Resulting Parquet files are either written to local filesystem or imported to HDFS

Motivation: The analysis of large CSV formatted Twitter datasets such as [this](https://about.twitter.com/en_us/values/elections-integrity.html#data).

### Build
```
$ git clone https://github.com/nwrs/hdfs-parquet-importer.git
$ cd hdfs-parquet-importer
$ mvn clean install
```
### Running
```
$java -jar hdfs-parquet-importer-1.0-SNAPSHOT-packaged.jar --srcFile /Users/nwrs/ira_tweets_csv_hashed.csv \
  --schemaFile /Users/nwrs/tweets.schema \
  --destFile hdfs://localhost:9000/trolls/tweets.parquet \
  --dateEnrich tweet_time \
  --partitionCols year,month \
  --sortCols hashtags
```
### Command Line Options

```
$java -jar hdfs-parquet-importer-1.0-SNAPSHOT-packaged.jar --help

HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]

Usage:

  -e, --dateEnrich  <date_time_col>          Using a date/time string column as a source add additional date/year/month
                                             columns to allow smarter partitioning.
  -l, --delimeter  <char>                    CSV delimeter character, default is ','.
  -d, --destFile   <path/file.parquet>       Destination Parquet file to export to. Local FS or HDFS paths supported.
  -p, --partitionCols  <column,column,...>   Partition columns.
  -f, --schemaFile </path/to/file.schema>    Schema file path.
  -q, --slashEscapes                         Use '\"' as an escape character instead of '""' to denote quotes within a quote.
  -o, --sortCols   <column,column,...>       Sort columns.
  -t, --threads    <n>                       Number of Spark threads, default is # processors.
  -s, --srcFile    </path/to/file.csv>       CSV file to import.
  -w, --twitterCleanse                       Remove corrupted rows in Twitter sourced CSV files.
  -h, --help                                 Show help message

```

### Schema Files

If no schema is provided it will be inferred. To use an explicit schema a config file should be provided:
* Schema file format is columnName=Type 
* Supported types are String, Long, Int, Double, Float or Boolean.
* Comment character is '\#'.
* Full example for a simple Twitter dataset [here](https://github.com/nwrs/hdfs-parquet-importer/blob/master/src/test/data/tweets.schema)


### Tips

For information on optimising Parquet for read performance see [this](https://www.slideshare.net/RyanBlue3/parquet-performance-tuning-the-missing-guide) from the Netflix engineering team.

For longer running imports the Spark GUI can be used to view progress, the default location is [http://localhost:4040/jobs/](http://nrs-macbook-pro:4040/jobs/)