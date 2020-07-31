package com.tbex.bd.spark.streaming

import java.sql.{Connection, ResultSet, Statement}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.ForeachWriter
import com.tbex.bd.spark.utils.{JsonHelper, PhoenixHelper}
import com.tbex.bd.spark.utils.Constant._
import scala.collection.JavaConversions._
import scala.collection.mutable

object PhoenixForeachWriter {

  def apply(): ForeachWriter[String] = {
    new PhoenixForeachWriter()
  }
}

class PhoenixForeachWriter() extends ForeachWriter[String] {
  private var connection: Connection = _
  private var statement: Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    connection = PhoenixHelper.getPhoenixConnection
    statement = connection.createStatement()
    true
  }

  override def process(value: String): Unit = {
    val dataList = JsonHelper.readJson(value)
    var sql: String = null
    dataList.foreach(data=>{
      try {
        //database, table, mysqlFieldType, sqlType, eachRow
        val database = data._1
        val table = data._2
        val mysqlFieldType = data._3
        val operaterType = data._4
        val mysqlData = data._5
        println("==========================================")
        println(database)
        println(table)
        println(mysqlFieldType)
        println(operaterType)
        println(mysqlData)

        val hiveTableName = HIVE_TABLE_PREFIX.concat(database).concat(HIVE_TABLE_CONN).concat(table)
        val ifTableExistsSql = s"select table_name from $PHOENIX_CONFIG_TABLE where database_name = '$database' and table_name='$table'"
        println("ifTableExistsSql : ".concat(ifTableExistsSql))
        val result = statement.executeQuery(ifTableExistsSql)
        var configTableName: String = ""
        while(result.next()) {
          configTableName = result.getString(1)
        }
        println("configTableName : ".concat(configTableName))
        if (configTableName.length != 0) {
          if ("INSERT".equals(operaterType) || "UPDATE".equals(operaterType)) {
            // upsert into person (id,sex) values(1, '女');
            println("upsert")

            val map: JSONObject = JSON.parseObject(mysqlData)
            val keySet = new StringBuilder()
            val valueSet = new StringBuilder()
            for (entry <- map.entrySet) {
              val key = entry.getKey
              println(key)
              val value = entry.getValue
              keySet.append(key).append(",")
              valueSet.append(fieldTrans(mysqlFieldType, key, value.toString)).append(",")
            }
            keySet.deleteCharAt(keySet.length - 1)
            valueSet.deleteCharAt(valueSet.length - 1)

            sql = s"upsert into $hiveTableName ($keySet) values ($valueSet)"
            println(sql)
            statement.execute(sql)
            connection.commit()
          }else if ("DELETE".equals(operaterType)) {
            //delete from person where name='zhangsan';
            println("delete")
            val ifPrimaryKeyExistsSql = s"select * from $PHOENIX_CONFIG_TABLE where database_name = '$database' and table_name='$table'"
            println("ifPrimaryKeyExistsSql : ".concat(ifPrimaryKeyExistsSql))
            val result = statement.executeQuery(ifPrimaryKeyExistsSql)
            var primaryKey: String = ""
            while(result.next()) {
              primaryKey = result.getString(5)
            }
            println(primaryKey)
            if (primaryKey.length != 0) {
              val map: JSONObject = JSON.parseObject(mysqlData)
              val deleteKey = fieldTrans(mysqlFieldType, primaryKey, map.get(primaryKey).toString)
              sql = s"delete from $hiveTableName where $primaryKey=$deleteKey"
              println(sql)
              statement.execute(sql)
              connection.commit()
            } else {
              println("primaryKey is null , can not delete !")
            }
          } else {
            println("unknow operaterType : ".concat(operaterType))
          }
        } else {
          println(hiveTableName.concat(" hive table is not exists!"))
        }
      }catch {
        case ex: Exception =>
          println(s"$ex")
      }
    })
  }

  //字段类型转换
  def fieldTrans(mysqlFieldType: String, key:String, value: String): String ={
    val fieldTypeMap: mutable.Map[String, String] = mutable.HashMap[String,String]()
    for (entry <- JSON.parseObject(mysqlFieldType).entrySet) {
      fieldTypeMap.put(entry.getKey,entry.getValue.toString)
    }
    val mysqlFieldValue = fieldTypeMap.get(key).toString
    if(mysqlFieldValue.toLowerCase.contains("bit")){
      if(value.equals("1")){
        return "\'true\'"
      }else{
        return "\'false\'"
      }
    }
    if(mysqlFieldValue.toLowerCase.contains("int") || mysqlFieldValue.toLowerCase.contains("float")
      || mysqlFieldValue.toLowerCase.contains("double")){ //|| mysqlFieldValue.toLowerCase.contains("decimal")
      value
    }else{
      "\'".concat(value).concat("\'")
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      statement.close()
    } catch {
      case ex: Exception =>
        println(s"close statementfailed, msg=$ex")
    }
    try {
      connection.close()
    } catch {
      case ex: Exception =>
        println(s"close statement failed, msg=$ex")
    }
    PhoenixHelper.close()
  }
}
