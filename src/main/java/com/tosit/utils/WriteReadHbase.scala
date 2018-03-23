package com.tosit.utils

import com.tosit.entity.{BehaviorUserApp, BehaviorUserDayTime, BehaviorUserHourAppTime, BehaviorUserHourTime}
import org.apache.hadoop.hbase.client.Connection
import org.json.JSONObject
import sun.util.resources.ga.LocaleNames_ga
import scala.util.parsing.json.JSONArray

class WriteReadHbase {
}

object WriteReadHbase{
  //向BehaviorUserApp类型数据库插入数据
  def writeToBUA(connection: Connection,behaviorUserApp: BehaviorUserApp,tableName:String): Unit ={
    var data:Map[String,Long] = behaviorUserApp.getData()
    data.keys.foreach{ i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i,behaviorUserApp.getUserId().toString +":"+ behaviorUserApp.getDay(),data(i).toString)
    }
    HbaseUtils.insertHTable(connection,tableName,"timelen","begintime",behaviorUserApp.getUserId().toString +":"+ behaviorUserApp.getDay(),behaviorUserApp.getBegintime().toString)
    HbaseUtils.insertHTable(connection,tableName,"timelen","endtime",behaviorUserApp.getUserId().toString +":"+ behaviorUserApp.getDay(),behaviorUserApp.getEndtime().toString)
  }

  //向BehaviorUserDayTime类型数据库插入数据
  def writeToBUDT(connection: Connection,behaviorUserDayTime: BehaviorUserDayTime,tableName:String): Unit ={
    var data:Map[Int,Long] = behaviorUserDayTime.getData()
    data.keys.foreach{i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i.toString,behaviorUserDayTime.getUserId().toString,data(i).toString)
    }
  }

  //向BehaviorUserHourAppTime类型数据库插入数据
  def writeToBUHAT(connection: Connection,behaviorUserHourAppTime: BehaviorUserHourAppTime,tableName:String): Unit ={
    var data:Map[Int,Long] = behaviorUserHourAppTime.getData()
    data.keys.foreach{i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen", i.toString,
        behaviorUserHourAppTime.getUserId().toString+":"+behaviorUserHourAppTime.getDay()+":"+behaviorUserHourAppTime.getApp(),
        data(i).toString)
    }
  }

  //向BehaviorUserHourTime类型数据库插入数据
  def writeToBUHT(connection: Connection,behaviorUserHourTime:BehaviorUserHourTime,tableName:String): Unit ={
    var data:Map[Int,Long] = behaviorUserHourTime.getData()
    data.keys.foreach{i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i.toString(),
        behaviorUserHourTime.getUserId()+":"+behaviorUserHourTime.getMonth(),data(i).toString)
    }
  }

  //从BehaviorUserApp类型数据库很据key获取数据存入对象
  def readFromBUA(connection: Connection,tableName:String,key:String):BehaviorUserApp = {
    var info= key.split(":").toIterator
    var userId = info.next()
    var day = info.next()
    var begintime:Long = 0
    var endtime:Long = 0
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[String,Long] = Map()
    for (rowKv <- result.raw()) {
      var qua = new String(rowKv.getQualifier)
      qua match{
        case "begintime" => begintime = (new String(rowKv.getValue)).toInt.toLong
        case "endtime" => endtime = (new String(rowKv.getValue)).toInt.toLong
        case _ => data += (qua -> (new String(rowKv.getValue)).toInt.toLong)
      }
    }
    var behaviorUserApp:BehaviorUserApp = new BehaviorUserApp(userId.toInt,day,begintime,endtime,data)
    return behaviorUserApp
  }

  //从BehaviorUserDayTime类型数据库很据key获取数据存入对象
  def readFromBUDT(connection: Connection,tableName:String,key:String):BehaviorUserDayTime = {
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[Int,Long] = Map()
    var ite = tableName.split("_").toArray
    var mon = ite(ite.length-1)
    println(mon)
    for (rowKv <- result.raw()){
      data += ((new String(rowKv.getQualifier)).toInt -> (new String(rowKv.getValue)).toInt.toLong)
    }
    var behaviorUserDayTime:BehaviorUserDayTime = new BehaviorUserDayTime(key.toInt,mon,data)
    return behaviorUserDayTime
  }
}
