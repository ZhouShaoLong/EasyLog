package com.tosit.utils

import com.tosit.entity._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import scala.util.parsing.json.JSON

class DataUtils {}

object DataUtils {
  def StringToMap(jsonStr: String): Map[String, Any] = {

    JSON.globalNumberParser = {
      in =>
        try in.toInt catch { case _: NumberFormatException => in.toDouble}
    }

    val jsonValue = JSON.parseFull(jsonStr)

    val jsonObj = jsonValue match {
      case Some(map: Map[String, Any]) => map
      case other => Map[String, Any]("Error JSON String" -> null)
    }
    jsonObj
  }

  def MapToEasyLog(map: Map[String, Any]): EasyLog = {
    val userId = map.apply("userId").toString.toDouble.toInt
    val day = map.apply("day").toString
    val begintime = map.apply("begintime").toString.toDouble.toLong
    val endtime = map.apply("endtime").toString.toDouble.toLong
    val data = map.apply("data").asInstanceOf[List[Map[String, Any]]]

    println(data)

    var _map: Map[String, Long] = Map()
    data.foreach(data=>{
      val key: String = data.apply("package").toString
      val value: Long = data.apply("activetime").toString.toDouble.toLong
      _map += (key -> value)
    })

    val easyLog = new EasyLog(userId, day, begintime, endtime, _map)
    easyLog
  }

  def DataProcess(str: String): EasyLog ={
    val map = StringToMap(str)
    val easyLog = MapToEasyLog(map)
    easyLog
  }

  def DataToHbase(easyLog: EasyLog): Unit ={
    val conf = HBaseConfiguration.create
    //val tablename = "blog"
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum", "hdp-node-01,hdp-node-02,hdp-node-03,hdp-node-04")
    // conf.set("hbase.zookeeper.quorum", "hadoop1.snnu.edu.cn,hadoop3.snnu.edu.cn")
    //conf.set(TableInputFormat.INPUT_TABLE, tablename)
    val connection = ConnectionFactory.createConnection(conf)
    ToHourTime(connection,easyLog)
    ToApp(connection,easyLog)
    ToDayTime(connection,easyLog)
    ToHourAppTime(connection,easyLog)
  }

  def ToHourTime(connection:Connection,easyLog: EasyLog): Unit ={
    val clock:Int = easyLog.getClock()
    var timeLen:Long = 0
    easyLog.getData().foreach(data=>{
      timeLen+=data._2
    })
    var behaviorUserHourTime = new BehaviorUserHourTime(easyLog.getUserId(), easyLog.getDay(),Map(clock->timeLen))
    if(HbaseUtils.ifExistsByColumn(connection,
      "behavior_user_hour_time_201702",
      behaviorUserHourTime.getUserId()+":"+behaviorUserHourTime.getDay(),
      behaviorUserHourTime.getData().keys.head.toString())) {
      var len = HbaseUtils.getValueByColumn(connection,
        "behavior_user_hour_time_201702",
        behaviorUserHourTime.getUserId()+":"+behaviorUserHourTime.getDay(),
        behaviorUserHourTime.getData().keys.head.toString()
      ).toLong + timeLen
      var behaviorUserHourTime2 = new BehaviorUserHourTime(behaviorUserHourTime.getUserId(),
        behaviorUserHourTime.getDay(),
        Map(behaviorUserHourTime.getData().keys.head.toString().toInt -> len))
      WriteReadHbase.writeToBUHT(connection,behaviorUserHourTime2,"behavior_user_hour_time_201702")
    }else{
      WriteReadHbase.writeToBUHT(connection,behaviorUserHourTime,"behavior_user_hour_time_201702")
    }
  }

  def ToHourAppTime(connection: Connection,easyLog: EasyLog): Unit ={
    val userId = easyLog.getUserId()
    val arr = easyLog.getDay().split("-").toArray
    val clock = easyLog.getClock()
    val data = easyLog.getData()
    val day = easyLog.getDay()

    val behaviorUserHourAppTime = new BehaviorUserHourAppTime(userId, easyLog.getDay(), clock, data)
    data.keys.foreach{i =>
      if(HbaseUtils.ifExistsByColumn(connection,
        "behavior_user_hour_app_time_201702",
        userId+":"+day + ":" +i,
        clock.toString)){
        val len = HbaseUtils.getValueByColumn(connection,
          "behavior_user_hour_app_time_201702",
          userId+":"+day + ":" +i,
          clock.toString).toLong + data(i).toLong
        HbaseUtils.updateColumn(connection,
          "behavior_user_hour_app_time_201702",
          userId+":"+day + ":" +i,
          clock.toString,len.toString)
      }else{
        HbaseUtils.updateColumn(connection,
          "behavior_user_hour_app_time_201702",
          userId+":"+day + ":" +i,
          clock.toString,data(i).toString)
      }
    }
  }

  def ToDayTime(connection: Connection,easyLog: EasyLog): Unit ={
    val date  = easyLog.getDay()
    val arr = date.toString.split("-").toArray
    val day = arr(2)
    val month = arr(0) + arr(1)
    var timeLen: Int = 0
    var userId = easyLog.getUserId()
    easyLog.getData().values.foreach{i=>
      timeLen += i.toInt
    }
    val behaviorUserDayTime = new BehaviorUserDayTime(easyLog.getUserId(), month, Map(day.toInt->timeLen.toLong))
    if(HbaseUtils.ifExistsByColumn(connection,
      "behavior_user_day_time_201702",
      userId+":"+month,
      day)){
      var len = HbaseUtils.getValueByColumn(connection,
        "behavior_user_day_time_201702",
        userId+":"+month,
        day).toLong + timeLen.toLong
      HbaseUtils.updateColumn(connection,
        "behavior_user_day_time_201702",
        userId+":"+month,
        day,len.toString)
    }else{
      HbaseUtils.updateColumn(connection,
        "behavior_user_day_time_201702",
        userId+":"+month,
        day,timeLen.toString)
    }
  }

  def ToApp(connection: Connection,easyLog: EasyLog): Unit ={
    val behaviorUserApp = new BehaviorUserApp(easyLog.getUserId(), easyLog.getDay(), easyLog.getData())
    val userId = behaviorUserApp.getUserId()
    val day = behaviorUserApp.getDay()
    val data = behaviorUserApp.getData()
    data.keys.foreach{i=>
      if(HbaseUtils.ifExistsByColumn(connection,
        "behavior_user_app_201702",
        userId+":"+day,
        i.toString())){
        var len = HbaseUtils.getValueByColumn(connection,
          "behavior_user_app_201702",
          userId+":"+day,
          i.toString()).toLong + data(i).toLong
        HbaseUtils.updateColumn(connection, "behavior_user_app_201702",userId+":"+day, i.toString(),len.toString)
      }else{
        HbaseUtils.updateColumn(connection, "behavior_user_app_201702",userId+":"+day, i.toString(),data(i).toString)
      }
    }
  }
}
