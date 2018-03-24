package com.tosit.utils

import com.tosit.entity.EasyLog
import com.tosit.entity.BehaviorUserHourTime

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
    ToHourTime(easyLog)
  }

  def ToHourTime(easyLog: EasyLog): Unit ={
    val clock:Int = easyLog.getClock()
    var timeLen:Long = 0
    easyLog.getData().foreach(data=>{
      timeLen+=data._2
    })

    val behaviorUserHourTime = new BehaviorUserHourTime(easyLog.getUserId(), easyLog.getDay(),Map(clock->timeLen))

    println("ToHourTime")
  }

  def ToHourAppTime(easyLog: EasyLog): Unit ={

  }

  def ToDayTime(easyLog: EasyLog): Unit ={

  }

  def ToApp(easyLog: EasyLog): Unit ={

  }

}
