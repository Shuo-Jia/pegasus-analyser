package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.client.SetItem
import org.apache.spark.rdd.RDD

object CustomImplicits {

  implicit def convertFromByte(rdd: RDD[SetItem]): PegasusOnlineRDD = new PegasusOnlineRDD(rdd)
}
