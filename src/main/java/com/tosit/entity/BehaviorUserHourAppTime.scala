package com.tosit.entity

/**
  * 对应同名数据库
  * */

class BehaviorUserHourAppTime(_userId:Int,_day:String,_app:String,_data:Map[Int,Long]) {
  var userId = _userId
  var day:String = _day
  var app:String = _app
  var data:Map[Int,Long] = _data

  def getUserId() = userId
  def getDay() = day
  def getApp() = app
  def getData() = data
}
