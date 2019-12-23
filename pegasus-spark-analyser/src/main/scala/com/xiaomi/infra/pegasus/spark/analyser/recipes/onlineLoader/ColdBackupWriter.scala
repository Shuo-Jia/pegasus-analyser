package com.xiaomi.infra.pegasus.spark.analyser.recipes.onlineLoader

import com.xiaomi.infra.pegasus.client.{ClientOptions, SetItem}
import com.xiaomi.infra.pegasus.spark.analyser.{ColdBackupConfig, ColdBackupLoader, OnlineLoaderOptions, PegasusContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.tools.ZstdWrapper

object ColdBackupWriter {

  def main(args: Array[String]): Unit = {
    val resourceConfig = new ColdBackupConfig

    resourceConfig
      .setRemote(
        "",
        "80")
      .setTableInfo("c3srv-browser", "browser_usercenter_monthlydata")

    val loaderOptions: OnlineLoaderOptions = OnlineLoaderOptions()
      .setMetaServer("")
      .setCluster("c4tst-tune")
      .setTable("jiashuo_bulkloader_from_cold")

    val conf: SparkConf = new SparkConf()
      .setAppName(
        "Online load data from \"%s\" in clusters \"%s\""
          .format(loaderOptions.cluster, loaderOptions.table)
      )
      .setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)

    new PegasusContext(sc)
      .pegasusSnapshotRDD(new ColdBackupLoader(resourceConfig))
      .map(i => {
        new SetItem(i.hashKey, i.sortKey, ZstdWrapper.compress(i.value))
      })
      .saveAsPegasus(loaderOptions)
  }

}
