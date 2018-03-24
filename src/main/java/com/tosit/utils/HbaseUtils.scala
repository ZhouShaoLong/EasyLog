package com.tosit.utils
import java.util.Iterator

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

import scala.util.control.Exception

class HbaseUtils {
}

object HbaseUtils{
  //创建表
  def createHTable(connection: Connection,tablename: String,families:Array[String]): Unit=
  {
    //Hbase表模式管理器
    val admin = connection.getAdmin
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //如果需要创建表
    if (!admin.tableExists(tableName)) {
      //创建Hbase表模式
      val tableDescriptor = new HTableDescriptor(tableName)
      for(family <- families){
        tableDescriptor.addFamily(new HColumnDescriptor(family.getBytes()))
      }
      //创建表
      admin.createTable(tableDescriptor)
      println("table created")
    }else{
      println("table exists")
    }
  }

  //删除表
  def deleteHTable(connection:Connection,tablename:String):Unit={
    //本例将操作的表名
    val tableName = TableName.valueOf(tablename)
    //Hbase表模式管理器
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }

  //插入记录
  def insertHTable(connection:Connection,tablename:String,family:String,column:String,key:String,value:String):Unit={
    try{
      val userTable = TableName.valueOf(tablename)
      val table=connection.getTable(userTable)
      //准备key 的数据
      val p=new Put(key.getBytes)
      //为put操作指定 column 和 value
      p.addColumn(family.getBytes,column.getBytes,value.getBytes())
      //验证可以提交两个clomun？？？？不可以
      // p.addColumn(family.getBytes(),"china".getBytes(),"JAVA for china".getBytes())
      //提交一行
      table.put(p)
    }catch {
      case ex:Exception =>{
        println(ex)
      }
    }
  }

  //基于KEY查询某条数据
  def getAResult(connection:Connection,tablename:String,family:String,column:String,key:String):Unit={
    var table:Table=null
    try{
      val userTable = TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val g=new Get(key.getBytes())
      val result=table.get(g)
      val value=Bytes.toString(result.getValue(family.getBytes(),column.getBytes()))
      println("key:"+value)
      if(table!=null)table.close()
    }catch{
      case ex:Exception=>{
        println(ex)
      }
    }
  }

  //根据key获取一行数据
  def getRow(connection:Connection,tableName: String, key: String):Result= {
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val get: Get = new Get(Bytes.toBytes(key))
    val result: Result = table.get(get)
    return result
/*    for (rowKv <- result.raw()) {
      println(new String(rowKv.getFamily))
      println(new String(rowKv.getQualifier))
      println(rowKv.getTimestamp)
      println(new String(rowKv.getRow))
      println(new String(rowKv.getValue))
      println("---------------------")
    }*/
  }

  //根据key和column获取一个数据
  def getValueByColumn(connection: Connection,tableName: String,key:String,column:String):String={
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val get: Get = new Get(Bytes.toBytes(key))
    get.addColumn(Bytes.toBytes("timelen"),Bytes.toBytes(column))
    val result: Result = table.get(get)
    var res:String = ""
    for (rowKv <- result.raw()) {
      res = new String(rowKv.getValue)
    }
    return res
  }

  //根据key展示一行数据，仅用于测试使用
  def showRow(connection:Connection,tableName: String, key: String):Unit= {
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val get: Get = new Get(Bytes.toBytes(key))
    val result: Result = table.get(get)
        for (rowKv <- result.raw()) {
          println(new String(rowKv.getFamily))
          println(new String(rowKv.getQualifier))
          println(rowKv.getTimestamp)
          println(new String(rowKv.getRow))
          println(new String(rowKv.getValue))
          println("---------------------")
        }
  }

  //根据key查询是否存在
  def ifExists(connection: Connection,tableName:String,key:String):Boolean = {
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val get: Get = new Get(Bytes.toBytes(key))
    val result: Result = table.get(get)
    if (result.isEmpty){
      return false
    }else{
      return true
    }
  }

  //根据key和colomn查询是否存在具体一列
  def ifExistsByColumn(connection: Connection,tableName: String,key:String,column:String):Boolean = {
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val get: Get = new Get(Bytes.toBytes(key))
    get.addColumn(Bytes.toBytes("timelen"),Bytes.toBytes(column))
    val result: Result = table.get(get)
    if(result.isEmpty){
      return false
    }else{
      return true
    }
  }

  //获取所有数据
  def getAllRows(connection: Connection,tableName: String): Unit = {
    val userTable = TableName.valueOf(tableName)
    val table:Table = connection.getTable(userTable)
    val results: ResultScanner = table.getScanner(new Scan())
    val it: Iterator[Result] = results.iterator()
    while (it.hasNext) {
      val next: Result = it.next()
      for(kv <- next.raw()){
        println(new String(kv.getRow))
        println(new String(kv.getFamily))
        println(new String(kv.getQualifier))
        println(new String(kv.getValue))
        println(kv.getTimestamp)
        println("---------------------")
      }
    }
  }

  //更新一个列
  def updateColumn(connection: Connection,tableName:String,key:String,column:String,value:String):Boolean = {
    try{
      val userTable = TableName.valueOf(tableName)
      val table=connection.getTable(userTable)
      //准备key 的数据
      val p=new Put(key.getBytes)
      //为put操作指定 column 和 value
      p.addColumn(Bytes.toBytes("timelen"),column.getBytes,value.getBytes())
      //提交一行
      table.put(p)
      return true
    }catch {
      case ex:Exception => {
        println(ex)
      }
        return false
    }
  }

  //删除某条记录
  def deleteRecord(connection:Connection,tablename:String,family:String,column:String,key:String): Unit ={
    var table:Table=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val d=new Delete(key.getBytes())
      d.addColumn(family.getBytes(),column.getBytes())
      table.delete(d)
      println("delete record done.")
    }finally{
      if(table!=null)table.close()
    }
  }

  //扫描记录
  def scanRecord(connection:Connection,tablename:String,family:String,column:String): Unit ={
    var table:Table=null
    var scanner:ResultScanner=null
    try{
      val userTable=TableName.valueOf(tablename)
      table=connection.getTable(userTable)
      val s=new Scan()
      s.addColumn(family.getBytes(),column.getBytes())
      scanner=table.getScanner(s)
      var result:Result=scanner.next()
      while(result!=null){
        println("Found row:" + result)
        println("Found value: "+Bytes.toString(result.getValue(family.getBytes(),column.getBytes())))
        result=scanner.next()
      }
    }finally{
      if(table!=null)
        table.close()
      scanner.close()
    }
  }
}
