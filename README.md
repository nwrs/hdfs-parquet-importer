## HDFS Parquet Importer

A tool to convert CSV files to Parquet format and import into HDFS.

* Converts from CSV to Parquet format.
* Supports either schema inference or the supply of a specific schema.
* Optionally sorts and repartitions by multiple columns to optimise for filtering and read performance.
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

### Full Cmd Line Options

```
$java -jar hdfs-parquet-importer-1.0-SNAPSHOT-packaged.jar --help

HDFS Parquet CSV file importer [github.com/nwrs/hdfs-parquet-importer]

For usage see below:
  -e, --dateEnrich  <date_time src col>      Enrich string formatted date/time
                                             col to a date/year/month columns to
                                             allow smarter partitioning
  -l, --delimeter  <arg>                     CSV delimeter character, default is
                                             ','
  -d, --destFile  <arg>                      Destination Parquet file to export
                                             to. Local FS or HDFS paths
                                             supported
  -p, --partitionCols  <column,column,...>   Partition columns
  -f, --schemaFile  </path/to/file.schema>   Schema file
  -q, --slashEscapes                         Use '\"' as an escape character
                                             instead of '""' to denote quotes
                                             within a quote
  -o, --sortCol  <column>                    Sort column
  -t, --sparkThreads  <arg>                  Numbner of Spark threads, default
                                             is # processors
  -s, --srcFile  </path/to/file.csv>         CSV file to import
  -h, --help                                 Show help message
```

### Schema Files

If no schema is provided schemas will be inferred by the data. To use an explicit schema provide schema a config file:
* Schema file format is columnName=Type 
* Supported types are String, Long, Int, Double, Float or Boolean.
* Comment character is '\#'.
* Full example for a simple Twitter dataset [here](https://github.com/nwrs/hdfs-parquet-importer/blob/master/src/test/data/tweets.schema)
