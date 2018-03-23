package com.tosit.entity

/**
  * 对应同名数据库
  * */

class BehaviorUserDayTime(_userId:Int,_month:String,_data:Map[Int,Long]) {
  var userId:Int = _userId
  var month:String = _month
  var data:Map[Int,Long] = _data

  def getUserId() = userId
  def getMonth() = month
  def getData() = data
}
