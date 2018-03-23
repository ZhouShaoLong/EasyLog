package com.tosit.log

import com.tosit.utils.DataUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
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
        "test",
        "2"
      )

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val topic = "test"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap


    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )

//    val stream = getStream(ssc, kafkaParams, topicMap)


//    val count = stream.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

//    count.print()

//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()


    val str = """{"username":"Ricky", "attribute":{"age":21, "weight": 60}}"""
    val map = DataUtils.StringToMap(str)
    val easyLog = DataUtils.MapToEasyLog(map)
    println(easyLog.getDay())


  }

  def getStream(ssc:StreamingContext, kafkaParams:Map[String, String], topicMap:Map[String,Int]): ReceiverInputDStream[(String, String)] ={
    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicMap,
      StorageLevel.MEMORY_AND_DISK_SER
    )
    stream
  }
}