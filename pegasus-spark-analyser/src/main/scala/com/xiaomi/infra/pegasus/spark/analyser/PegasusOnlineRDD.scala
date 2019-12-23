package com.xiaomi.infra.pegasus.spark.analyser

import java.time.Duration

import com.xiaomi.infra.pegasus.client.{
  ClientOptions,
  PException,
  PegasusClientFactory,
  SetItem
}
import com.xiaomi.infra.pegasus.tools.FlowController
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class OnlineLoaderOptions() extends Serializable {
  var metaServer: String = _
  var timeout: Long = _
  var asyncWorks: Int = _

  var cluster: String = _
  var table: String = _
  var bulkNum: Int = 10
  var flowControl: Int = 5

  def setMetaServer(metaServer: String): OnlineLoaderOptions = {
    this.metaServer = metaServer
    this
  }

  def setTimeout(timeout: Long): OnlineLoaderOptions = {
    this.timeout = timeout
    this
  }

  def setAsyncWorks(asyncWorks: Int): OnlineLoaderOptions = {
    this.asyncWorks = asyncWorks
    this
  }

  def setCluster(cluster: String): OnlineLoaderOptions = {
    this.cluster = cluster
    this
  }

  def setTable(table: String): OnlineLoaderOptions = {
    this.table = table
    this
  }

  def setBulkNum(bulkNum: Int): OnlineLoaderOptions = {
    this.bulkNum = bulkNum
    this
  }

  def setFlowControl(flowControl: Int): OnlineLoaderOptions = {
    this.bulkNum = bulkNum
    this
  }

}

class PegasusOnlineRDD(resource: RDD[SetItem]) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[PegasusOnlineRDD])

  def saveAsPegasus(loaderOptions: OnlineLoaderOptions): Unit = {
    resource.foreachPartition(i => {
      var count = 0

      val client = PegasusClientFactory.getSingletonClient(
        ClientOptions
          .builder()
          .metaServers(loaderOptions.metaServer)
          .operationTimeout(Duration.ofMillis(loaderOptions.timeout))
          .build())

      val flowController = new FlowController(loaderOptions.flowControl)
      val index = TaskContext.getPartitionId()
      i.sliding(loaderOptions.bulkNum, loaderOptions.bulkNum)
        .foreach(slice => {
          flowController.getToken()
          var success = false
          while (!success) {
            try {
              client.batchSet(loaderOptions.table, slice.asJava)
              success = true
            } catch {
              case ex: PException =>
                logger.info(
                  "partition index " + index + ": batchSet error:" + ex)
                Thread.sleep(10)
            }
            count += slice.size
            if (count % 10000 == 0) {
              logger.info(
                "partition index " + index + ": batchSet count:" + count)
            }
          }
        })
      logger.info("partition index " + index + ": total batchSet count:" + count)
      flowController.stop()
      client.close()
    })
  }
}
