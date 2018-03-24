package com.tosit.utils

import com.tosit.entity.{BehaviorUserApp, BehaviorUserDayTime, BehaviorUserHourAppTime, BehaviorUserHourTime}
import org.apache.hadoop.hbase.client.Connection

class WriteReadHbase {
}

object WriteReadHbase{
  //向BehaviorUserApp类型数据库插入数据
  def writeToBUA(connection: Connection,behaviorUserApp: BehaviorUserApp,tableName:String): Unit ={
    var data:Map[String,Long] = behaviorUserApp.getData()
    data.keys.foreach{ i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i,behaviorUserApp.getUserId().toString +":"+ behaviorUserApp.getDay(),data(i).toString)
    }
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
    var data:Map[String,Long] = behaviorUserHourAppTime.getData()
    data.keys.foreach{i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen", behaviorUserHourAppTime.getClock().toString,
        behaviorUserHourAppTime.getUserId().toString+":"+behaviorUserHourAppTime.getDay()+":"+i,
        data(i).toString)
    }
  }

  //向BehaviorUserHourTime类型数据库插入数据
  def writeToBUHT(connection: Connection,behaviorUserHourTime:BehaviorUserHourTime,tableName:String): Unit ={
    var data:Map[Int,Long] = behaviorUserHourTime.getData()
    data.keys.foreach{i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i.toString(),
        behaviorUserHourTime.getUserId()+":"+behaviorUserHourTime.getDay(),data(i).toString)
    }
  }

  //从BehaviorUserApp类型数据库很据key获取数据存入对象
  def readFromBUA(connection: Connection,tableName:String,key:String):BehaviorUserApp = {
    var info= key.split(":").toIterator
    var userId = info.next()
    var day = info.next()
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[String,Long] = Map()
    for (rowKv <- result.raw()) {
      data += (new String(rowKv.getQualifier) -> (new String(rowKv.getValue)).toDouble.toLong)
    }
    var behaviorUserApp:BehaviorUserApp = new BehaviorUserApp(userId.toInt,day,data)
    return behaviorUserApp
  }

  //从BehaviorUserDayTime类型数据库很据key获取数据存入对象
  def readFromBUDT(connection: Connection,tableName:String,key:String):BehaviorUserDayTime = {
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[Int,Long] = Map()
    var ite = tableName.split("_").toArray
    var mon = ite(ite.length-1)
    for (rowKv <- result.raw()){
      data += ((new String(rowKv.getQualifier)).toInt -> (new String(rowKv.getValue)).toDouble.toLong)
    }
    var behaviorUserDayTime:BehaviorUserDayTime = new BehaviorUserDayTime(key.toInt,mon,data)
    return behaviorUserDayTime
  }

  //从BehaviorUserHourTime类型数据库很据key获取数据存入对象
  def readFromBUHT(connection: Connection,tableName:String,key:String):BehaviorUserHourTime = {
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[Int,Long] = Map()
    var ite = key.split(":").toArray
    var userId = ite(0).toInt
    var day = ite(1)
    for (rowKv <- result.raw()){
      data += ((new String(rowKv.getQualifier)).toInt -> (new String(rowKv.getValue)).toDouble.toLong)
    }
    var behaviorUserHourTime:BehaviorUserHourTime = new BehaviorUserHourTime(userId,day,data)
    return behaviorUserHourTime
  }

  //从BehaviorUserHourAppTime类型数据库很据key获取数据存入对象
  def readFromBUHAT(connection: Connection,tableName:String,key:String,clock:Int):BehaviorUserHourAppTime = {
    var result = HbaseUtils.getRow(connection,tableName,key)
    var data:Map[String,Long] = Map()
    var ite = key.split(":").toArray
    var userId = ite(0).toInt
    var day = ite(1)
    var app = ite(2)
    for (rowKv <- result.raw()){
      var col = new String(rowKv.getQualifier)
      if(col.compare(clock.toString)==0){
        data += (app -> (new String(rowKv.getValue)).toDouble.toLong)
      }
    }
    var behaviorUserHourAppTime:BehaviorUserHourAppTime = new BehaviorUserHourAppTime(userId,day,clock,data)
    return behaviorUserHourAppTime
  }
}
