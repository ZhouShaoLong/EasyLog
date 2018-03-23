package com.tosit.entity

class BehaviorUserDayTime(_userId:Int,_month:String,_data:Map[Int,Long]) {
  var userId:Int = _userId
  var month:String = _month
  var data:Map[Int,Long] = _data

  def gerUserId() = userId
  def getMonth() = month
  def getData() = data
}
