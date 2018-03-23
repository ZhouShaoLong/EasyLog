package com.tosit.utils

import com.tosit.entity.BehaviorUserApp
import org.apache.hadoop.hbase.client.Connection

class WriteReadHbase {
}

object WriteReadHbase{
  //向BehaviorUserApple类型数据库插入数据
  def writeToBUA(connection: Connection,behaviorUserApp: BehaviorUserApp,tableName:String): Unit ={
    var data:Map[String,Long] = behaviorUserApp.getData()
    data.keys.foreach{ i=>
      HbaseUtils.insertHTable(connection,tableName,"timelen",i,behaviorUserApp.getUserId().toString,data(i).toString)
    }
  }
}
