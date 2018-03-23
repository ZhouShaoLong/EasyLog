package com.tosit.utils

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream


class KafkaUtils {

}

object KafkaUtils {
  def getStream(ssc: StreamingContext, kafkaParams: Map[String, String], topicMap: Map[String, Int]): ReceiverInputDStream[(String, String)] = {
    val stream = org.apache.spark.streaming.kafka.KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      topicMap,
      StorageLevel.MEMORY_AND_DISK_SER
    )
    stream
  }
}
