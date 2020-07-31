package com.tbex.bd.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import com.tbex.bd.spark.utils.Constant._

object SparkHelper {

  private var singleSparkSession: SparkSession = _

  def getSparkSession: SparkSession = {
    synchronized {
      if (singleSparkSession == null) {
        val conf = new SparkConf()
          .setAppName("mysql_to_ods_realtime")

        singleSparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
        singleSparkSession.sparkContext.setCheckpointDir(Constant.PATH_CHECKPOINT + "spark")
      }
    }
    singleSparkSession
  }

  def close(): Unit = {
    if (singleSparkSession != null) {
      try {
        singleSparkSession.close()
      } catch {
        case ex: Exception =>
          println(s"close singled sparksession failed, msg=$ex")
      }
    }
  }

  def stopByMarkFile(sq:StreamingQuery):Unit= {
    val conf = new Configuration()
    val path=new Path(CLOSE_STREAMING_HDFS_PATH)
    val fs =path.getFileSystem(conf)
    fs.mkdirs(path)

    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false

    while (!isStop) {
      isStop = sq.awaitTermination(intervalMills)
      if (!isStop && !fs.exists(path)) {
        println("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        sq.stop()
        close()
      }
    }
  }
}
