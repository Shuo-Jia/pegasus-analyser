package com.xiaomi.infra.pegasus.spark.bulkloader

import CustomImplicits._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.rocksdb.RocksDB

import scala.collection.JavaConverters._

class RocksDBRDD(rdd: RDD[(Array[Byte], Array[Byte], Array[Byte])]) {

  def saveAsSSTFile(config: BulkLoaderConfig): Unit = {
    val recordRDD = rdd.map(i => {
      (RocksDBRecord.create(i._1, i._2, i._3), "1")
    })

    val sstRDD = if (config.isDistinct) {
      recordRDD
        .distinct(config.tablePartitionCount)
        .repartitionAndSortWithinPartitions(
          new PegasusHashPartitioner(config.tablePartitionCount))
    } else
      recordRDD.repartitionAndSortWithinPartitions(
        new PegasusHashPartitioner(config.tablePartitionCount))

    sstRDD.foreachPartition(i => {
      RocksDB.loadLibrary()
      new BulkLoader(config, i.asJava, TaskContext.getPartitionId()).write()
    })
  }

}