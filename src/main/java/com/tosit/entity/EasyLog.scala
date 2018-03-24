package com.tosit.entity


class EasyLog(_userId: Int, _day: String, _begintime: Long, _endtime: Long, _data: Map[String, Long]) extends java.io.Serializable {
  private val userId: Int = _userId
  private val day: String = _day
  private val begintime: Long = _begintime
  private val endtime: Long = _endtime
  private val clock:Int = 1
  private val data: Map[String, Long] = _data

  def getUserId() = userId
  def getDay() = day
  def getBegintime() = begintime
  def getEndtime() = endtime
  def getData() = data
}

