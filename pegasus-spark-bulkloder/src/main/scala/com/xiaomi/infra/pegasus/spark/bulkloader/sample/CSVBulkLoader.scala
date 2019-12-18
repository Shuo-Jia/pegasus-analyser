package com.xiaomi.infra.pegasus.spark.bulkloader.sample
import com.xiaomi.infra.pegasus.spark.Config
import com.xiaomi.infra.pegasus.spark.bulkloader.{BulkLoaderConfig, RocksDBRecord}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data analyse")
      .setIfMissing("spark.master", "local[1]")
      .set("spark.executor.instances", "8")

    val sc = new SparkContext(conf)

    val sstConfig = new BulkLoaderConfig()

    sstConfig
      .setDistinct(false)
      .setRemote("", "80")
      .setTableInfo("C2", "T2")
      // TODO tableId and partitionCount should be get just by clusterName and tableName
      .setTableId(20)
      .setTablePartitionCount(32)

    //val data = sc.textFile("hdfs://hadoop/tmp/pegasus-dev/jiashuo/data.csv")
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        (RocksDBRecord.create(lines(0),lines(1),lines(2)),"")
      })
      .saveAsSSTFile(sstConfig)
  }

}
