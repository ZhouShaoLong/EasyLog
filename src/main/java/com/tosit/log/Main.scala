package com.tosit.log


import com.tosit.utils.{DataUtils, KafkaUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("stocker").setMaster("local[4]")
    val spark = new SparkContext(conf)
    val ssc = new StreamingContext(spark, Seconds(5))

    val Array(zkQuorum, group, topics, numThreads) =
      Array("hdp-node-01:2181,hdp-node-02:2181,hdp-node-03:2181,hdp-node-04:2181",
        "zookeeperGroup",
        "testkafka",
        "2"
      )

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )

    val stream = KafkaUtils.getStream(ssc, kafkaParams, topicMap)
    val str = stream.map(_._2)

    val easyLog = str.flatMap(_.split("\n")).map(DataUtils.DataProcess)
    easyLog.map(_.getData()).print()


    str.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}