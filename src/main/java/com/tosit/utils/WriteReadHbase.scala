package com.tosit.utils

import com.tosit.entity.{BehaviorUserApp, BehaviorUserDayTime, BehaviorUserHourAppTime, BehaviorUserHourTime}
import org.apache.hadoop.hbase.client.Connection
import sun.util.resources.ga.LocaleNames_ga

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
}
