package com.tbex.bd.spark.utils

class Constant {
}

object Constant {
  final val PHOENIX_JDBC = ""
  final val PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"

  final val HBASE_TEST_TABLE = "TEST:TEST_HBASE"
  final val DEFAULT_CF = "0"

  final val OUTPUT_MODE = "update"

  final val DATABASE = "database"
  final val TABLE = "table"
  final val TYPE = "type"
  final val MYSQL_FIELD_TYPE = "mysqlType"
  final val DATA = "data"
  final val KEY = "key"

  //  final val DB_TABLE = "pub.table_columns"
  final val DB_TABLE = "test.table_columns"
  final val BD_KEY = "database_name"
  final val TABLE_KEY = "table_name"
  final val PRIMARY_KEY = "primary_key"

  //phienix table
  final val HIVE_TABLE_PREFIX = "hive.bd_"
  final val HIVE_TABLE_CONN = "_bd_"
  final val PHOENIX_CONFIG_TABLE = "meta.table_columns"

  final val REPARTITION = 3
  final val HIVE_SOURCE = "hive"
  final val KAFKA_SOURCE = "kafka"
  final val TEXT_SOURCE = "text"
  final val CONSOLE_SOURCE = "console"

  final val HDFS = "hdfs://bigdata.t01.58btc.com"
  final val PATH_CHECKPOINT = HDFS + "/tmp/checkpoint_realtime/"
  final val CLOSE_STREAMING_HDFS_PATH = PATH_CHECKPOINT + "sparkstream_mark_path"
}