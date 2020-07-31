package com.tbex.bd.spark.utils

import Constant._
import com.alibaba.fastjson.JSON

import scala.collection.mutable.ListBuffer

object JsonHelper {

  def jsonValidate(jsonStr: String): Boolean = {
    var flag = false
    try {
      val map = JSON.parseObject(jsonStr)
      if (map.containsKey(DATABASE) &&
        map.containsKey(TABLE) &&
        map.containsKey(TYPE) &&
        map.containsKey(DATA)) {
        flag = true
      }
    } catch {
      case ex: Exception =>
        println(s"$ex")
    }
    flag
  }

  def readJson(jsonStr: String): List[(String, String, String, String, String)] = {
    val valueList = new ListBuffer[(String, String, String, String, String)]()
    if (jsonValidate(jsonStr)) {
      val map = JSON.parseObject(jsonStr)

      val database = {
        val temp = map.getString(DATABASE)
        if (temp == null) "none" else temp
      }
      val table = {
        val temp = map.getString(TABLE)
        if (temp == null) "none" else temp
      }

      val sqlType = {
        val temp = map.getString(TYPE)
        if (temp == null) "none" else temp
      }


      val mysqlFieldType = {
        val temp = map.getString(MYSQL_FIELD_TYPE)
        if (temp == null) "none" else temp
      }

      val dataArray = map.getJSONArray(DATA)
      if (dataArray == null) {
        return List.empty[(String, String, String, String, String)]
      }
      val it = dataArray.iterator()

      while (it.hasNext) {
        val eachRow = it.next().toString
        valueList.append((database, table, mysqlFieldType, sqlType, eachRow))
      }
      valueList.toList
    } else {
      valueList.append(("invalid", "", "", "", jsonStr))
      valueList.toList
    }
  }
}
