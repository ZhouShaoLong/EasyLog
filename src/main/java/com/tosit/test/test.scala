package com.tosit.test

import com.tosit.utils.HbaseUtils
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
        HbaseUtils.getRow(connection,"blog","002")
      } finally {
        connection.close
        //sc.stop
      }
    }
  }
}
