package com.tosit.entity


class EasyLog(_userId: Int, _day: String, _begintime: Long, _endtime: Long, _data: List[Map[String, Any]]) extends java.io.Serializable {
  private val userId: Int = _userId
  private val day: String = _day
  private val begintime: Long = _begintime
  private val endtime: Long = _endtime
  private val data: List[Map[String, Any]] = _data
  private val palytime: Long = endtime - begintime

  def getUserId() = userId

  def getDay() = day

  def getBegintime() = begintime

  def getEndtime() = endtime

  def getData() = data
}

