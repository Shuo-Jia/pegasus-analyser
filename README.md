# Pegasus-Spark

Pegasus-Spark is the [Spark](https://spark.apache.org/) connector to Pegasus. We've provided several toolkits for
manipulate your Pegasus data:
- pegasus-analyser: pegasus-analyser can read the pegasus snapshot data stored in the remote filesystem
  - Offline analysis of your Pegasus snapshot.
  - Transform your Pegasus snapshot into Parquet files.
  - Compare your data which stored in two different pegasus clusters.
- pegasus-bulkloader: pegasus-bulkloader can load the source data into pegasus cluster(need [pegasus server 2.1](https://github.com/apache/incubator-pegasus/tree/v2.1) support)



