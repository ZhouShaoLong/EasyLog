package com.tosit.entity


class EasyLog(_userId: Int, _day: String, _begintime: Long, _endtime: Long, _data: Map[String, Long]) {
  var userId: Int = _userId
  var day: String = _day
  var begintime: Long = _begintime
  var endtime: Long = _endtime
  var data: Map[String, Long] = _data

  def getUserId() = userId

  def getDay() = day

  def getBegintime() = begintime

  def getEndtime() = endtime

  def getData() = data
}

