package com.xiaomi.infra.pegasus.spark.bulkloader

import CustomImplicits._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.rocksdb.RocksDB

import scala.collection.JavaConverters._

class PegasusRecordRDD(data: RDD[(PegasusRecord, String)]) {

  def saveAsPegasusFile(config: BulkLoaderConfig): Unit = {
    var rdd = data
    if (config.enableDistinct) {
      rdd = rdd.distinct()
    }

    if (config.enableSort) {
      rdd = rdd.repartitionAndSortWithinPartitions(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    } else {
      rdd = rdd.partitionBy(
        new PegasusHashPartitioner(config.getTablePartitionCount)
      )
    }

    rdd.foreachPartition(i => {
      RocksDB.loadLibrary()
      new BulkLoader(config, i.asJava, TaskContext.getPartitionId()).start()
    })
  }

}
