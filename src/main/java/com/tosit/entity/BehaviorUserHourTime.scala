package com.tosit.entity

class BehaviorUserHourTime(_userId:Int,_day:String,_data:Map[Int,Long]) {
  var userId:Int = _userId
  var day:String = _day
  var data:Map[Int,Long] = _data

  def getUserId() = userId
  def getMonth() = day
  def getData() = data
}
