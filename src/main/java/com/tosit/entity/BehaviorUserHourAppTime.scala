package com.tosit.entity

/**
  * 对应同名数据库
  * */

class BehaviorUserHourAppTime(_userId:Int,_day:String,_clock:Int,_data:Map[String,Long]) {
  var userId = _userId
  var day:String = _day
  var clock:Int = _clock
  var data:Map[String,Long] = _data

  def getUserId() = userId
  def getDay() = day
  def getClock() = clock
  def getData() = data
}
