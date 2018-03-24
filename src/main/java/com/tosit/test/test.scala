package com.tosit.test

import com.tosit.utils.{HbaseUtils, WriteReadHbase}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object test {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create
    val tablename = "blog"
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum", "hdp-node-01,hdp-node-02,hdp-node-03,hdp-node-04")
    // conf.set("hbase.zookeeper.quorum", "hadoop1.snnu.edu.cn,hadoop3.snnu.edu.cn")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)
    try {
      //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
      val connection = ConnectionFactory.createConnection(conf)
      //创建表测试
      val f: Array[String] = Array("article")
      try {
        //HbaseUtils.createHTable(connection, "blog", f)
        //插入数据,重复执行为覆盖
        //HbaseUtils.insertHTable(connection, "blog", "article", "engish", "002", "c++ for me")
        //HbaseUtils.insertHTable(connection, "blog", "article", "engish", "003", "python for me")
        //HbaseUtils.insertHTable(connection, "blog", "article", "chinese", "002", "C++ for china")
        //删除记录
        // deleteRecord(connection,"blog","artitle","chinese","002")
        //扫描整个表
        //HbaseUtils.scanRecord(connection, "blog", "article", "engish")
        //删除表测试
        //deleteHTable(connection, "blog")
        //HbaseUtils.getRow(connection,"behavior_user_app_201702","2000:20170202")

/*        val sites = Map("runoob" -> 1000l,
          "baidu" -> 1000l,
          "taobao" -> 1000l)
        var behaviorUserApp:BehaviorUserApp = new BehaviorUserApp(3000,"20170202",1000,2000,sites)
        WriteReadHbase.writeToBUA(connection,behaviorUserApp,"behavior_user_app_201702")*/

/*        val site2 = Map(6 -> 1000l,
          7 -> 1000l,
          8 -> 1000l)
        var behaviorUserDayTime:BehaviorUserDayTime = new BehaviorUserDayTime(2000,"201702",site2)
        WriteReadHbase.writeToBUDT(connection,behaviorUserDayTime,"behavior_user_day_time_201702")*/

/*        val site3 = Map(6 -> 1000l,
          7 -> 1000l,
          8 -> 1000l)
        var behaviorUserHourAppTime:BehaviorUserHourAppTime = new BehaviorUserHourAppTime(2000,"201702","broswer1",site3)
        WriteReadHbase.writeToBUHAT(connection,behaviorUserHourAppTime,"behavior_user_hour_app_time_201702")*/

/*        val site4 = Map(6 -> 1000l,
          7 -> 1000l,
          8 -> 1000l)
        var behaviorUserHourTime:BehaviorUserHourTime = new BehaviorUserHourTime(2000,"201702",site4)
        WriteReadHbase.writeToBUHT(connection,behaviorUserHourTime,"behavior_user_hour_time_201702")*/

        //HbaseUtils.showRow(connection,"behavior_user_day_time_201702","2000")

/*        var res = WriteReadHbase.readFromBUA(connection,"behavior_user_app_201702","3000:20170202")
        println(res.getBegintime())
        println(res.getEndtime())
        println(res.getDay())
        println(res.getData())*/

/*        var res = WriteReadHbase.readFromBUHT(connection,"behavior_user_hour_time_201702","2000:201702")
        println(res.getUserId())
        println(res.getData())
        println(res.getDay())*/

/*        var res = WriteReadHbase.readFromBUHAT(connection,"behavior_user_hour_app_time_201702","2000:201702:broswer1")
        println(res.getApp())
        println(res.getDay())
        println(res.getUserId())
        println(res.getData())*/

/*        var res = HbaseUtils.ifExists(connection,"behavior_user_hour_app_time_201702","2000:201702:broswer1")
        println(res)*/

        /*var res = HbaseUtils.ifExistsByColumn(connection,"behavior_user_hour_app_time_201702","2000:201702:broswer1","6")
        println(res)*/

        var res2 = HbaseUtils.updateColumn(connection,"behavior_user_hour_app_time_201702","2000:201702:broswer1","6","3000")
        println(res2)


        var res = HbaseUtils.getValueByColumn(connection,"behavior_user_hour_app_time_201702","2000:201702:broswer1","6")
        println(res)

      } finally {
        connection.close
        //sc.stop
      }
    }
  }
}
