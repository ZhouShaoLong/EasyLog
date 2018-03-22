package com.tosit.utils

import com.tosit.entity.EasyLog
import scala.util.parsing.json.JSON

class DataUtils {}

object DataUtils {
  def StringToMap(jsonStr: String): Map[String, Any] = {
    val jsonValue = JSON.parseFull(jsonStr)

    val jsonObj = jsonValue match {
      case Some(map: Map[String, Any]) => map
      case other => Map[String, Any]("Error JSON String" -> null)
    }
    jsonObj
  }

  def MapToEasyLog(map: Map[String, Any]): EasyLog = {
    val userId = map.get("username").toString
    val attribute = map.apply("attribute").asInstanceOf[Map[String, String]]
    val age = attribute

    println(age)
    val easyLog = new EasyLog(1, userId, 1000, 1000, Map("aaa" -> 100))
    easyLog
  }

//  def getHbaseData(): EasyLog = {
//  }

//  def updateUserData: Unit ={
//
//  }


}
