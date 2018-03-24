package com.tosit.entity

/**
  * 对应同名数据库
  * */

class BehaviorUserApp(_userId:Int,_day:String, _data: Map[String, Long]) {
  var userId:Int = _userId
  var day:String = _day
  var data: Map[String, Long] = _data

  def getUserId() = userId
  def getDay() = day
  def getData() = data
}
