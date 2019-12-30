package com.xiaomi.infra.pegasus.spark.bulkloader.sample
import com.xiaomi.infra.pegasus.spark.Config
import com.xiaomi.infra.pegasus.spark.analyser.{ColdBackupConfig, ColdBackupLoader, PegasusContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.{BulkLoaderConfig, RocksDBRecord}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)

    val config = new BulkLoaderConfig()

    config
      .setDistinct(false)
      .setRemote("", "80")
      .setTableInfo("C2", "T2")
      // TODO tableId and partitionCount should be get just by clusterName and tableName
      .setTableId(20)
      .setTablePartitionCount(32)

    //val data = sc.textFile("hdfs://hadoop/tmp/pegasus-dev/jiashuo/data.csv")

    val coldConfig = new ColdBackupConfig
    coldConfig
      .setPolicyName("500g")
      .setRemote("","80")
      .setTableInfo("c4tst-fd","injection0")
      .setDbReadAheadSize(10)

    //only test
    //val rdd = sc.textFile("data.csv")
      val rdd = new PegasusContext(sc).pegasusSnapshotRDD(new ColdBackupLoader(coldConfig))

      rdd.map(i => {
        (RocksDBRecord.create(i.hashKey,i.sortKey,i.value),"")
      })
      .saveAsSSTFile(config)
  }

}
