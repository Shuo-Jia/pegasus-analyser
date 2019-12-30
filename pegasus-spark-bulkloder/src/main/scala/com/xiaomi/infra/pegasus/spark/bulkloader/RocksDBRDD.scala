package com.xiaomi.infra.pegasus.spark.bulkloader

import CustomImplicits._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.rocksdb.RocksDB

import scala.collection.JavaConverters._

class RocksDBRDD(rdd: RDD[(RocksDBRecord,String)]) {

  def saveAsSSTFile(config: BulkLoaderConfig): Unit = {

    val sstRDD = if (config.isDistinct) {
      rdd
        .distinct(config.tablePartitionCount)
        .repartitionAndSortWithinPartitions(
          new PegasusHashPartitioner(config.tablePartitionCount))
    } else
      rdd.repartitionAndSortWithinPartitions(
        new PegasusHashPartitioner(config.tablePartitionCount))

    sstRDD.foreachPartition(i => {
      var count = 0
      if(count % 100 == 0){
        count = count + 1
        println(count)
      }else{
        print(count)
      }
      /*RocksDB.loadLibrary()
      new BulkLoader(config, i.asJava, TaskContext.getPartitionId()).write()*/
    })
  }

}
