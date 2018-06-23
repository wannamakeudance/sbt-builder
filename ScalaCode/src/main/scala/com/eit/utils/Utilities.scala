package com.eit.utils

import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.SparkSession
import java.math.BigDecimal

import org.apache.spark.SparkContext
import org.apache.hadoop.fs._
import org.joda.time.Hours


object Utilities {


  def pr_Collect_Log(sc: SparkContext, spark: SparkSession, p_file_name: String, serialNo: Int, stage: String, time_taken: String, v_keyspace: String) {
    import  spark.implicits._
    val log = Seq((serialNo, p_file_name, stage, time_taken))
    val df_log=log.toDF("serial_no", "filename", "stage", "time_taken")
    df_log.repartition(1).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "logs", "keyspace" -> v_keyspace)).mode("append").save()

  }


  def pr_Collect_hourly_status(spark: SparkSession,hours: String, v_keyspace: String) {
    import  spark.implicits._
    val hour = hours +":00:00"
    val day = hours.substring(0,10)
    val log = Seq((hour,day,"completed"))
    val df_log=log.toDF("hour", "day", "status")
    df_log.repartition(1).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "hourly_agg_status", "keyspace" -> v_keyspace)).mode("append").save()

  }

  def pr_Collect_daily_status(spark: SparkSession,day: String, v_keyspace: String) {
    import  spark.implicits._
    val log = Seq((day,"completed"))
    val df_log=log.toDF("day", "status")
    df_log.repartition(1).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "daily_agg_status", "keyspace" -> v_keyspace)).mode("append").save()

  }

  def pr_move_File(sc : SparkContext,v_S3fs: org.apache.hadoop.fs.FileSystem, p_file_name:String, p_dir_src:String, p_dir_dst:String) : String =
  {


    v_S3fs.rename(new Path(p_dir_src+p_file_name), new Path(p_dir_dst+p_file_name))				// move file
    v_S3fs.delete(new Path(p_dir_src+p_file_name), true)

    ""
  }


  def removeLastDelimiter(str: String, Delimiter: String, noChar: Int): String = {
    var rStr: String = ""
    rStr = str.patch(str.lastIndexOf(Delimiter), "", noChar)
    rStr
  }

  def isString(value: String): String = {
    try {
      if (null != value && !value.isEmpty()) {
        value.toString
      }
      else
      {
        ""
      }
    } catch {
      case _: java.lang.NullPointerException=> ""

    }

  }
  def isFloat(value: String): Float = {
    try {
      if (null != value && !value.isEmpty()) {
        value.toFloat
      }
      else
      {
        "0.0".toFloat
      }
    } catch {
      case _: java.lang.NumberFormatException => "0.0".toFloat
      case _: java.lang.NullPointerException=> "0.0".toFloat

    }

  }

  def isDouble(s: String): Double = {
    try {
      var v = s.trim()
      if (null != v && !v.isEmpty()) {
        v.toDouble.toString

      }
      v.toDouble
    } catch {
      case _: java.lang.NumberFormatException => 0.0
      case _: java.lang.NullPointerException=> 0.0

    }

  }
  def isInt(s: String): Integer = {
    try {
      var v = s.trim()
      if (null != v && !v.isEmpty()) {
        v.toInt
      }
      v.toInt
    } catch {
      case _: java.lang.NumberFormatException => 0
      case _: java.lang.NullPointerException=> 0

    }

  }

  def isBigInt(s: String): String = {
    try {
      var v = s.trim()
      if (null != v && !v.isEmpty()) {
        v = new BigDecimal(v).toPlainString()

      }
      v
    } catch {
      case _: java.lang.NumberFormatException => 0.toString
      case _: java.lang.NullPointerException=> 0.toString

    }

  }
  def dataTypeConversion (Spark: SparkSession, table_name : String, keySpace : String): String = {
    Spark.read.cassandraFormat("columns", "system_schema").load().createOrReplaceTempView("columns")
    var ColumnQuery = " select column_name,type from columns where table_name = '"+table_name+"' and keyspace_name='"+ keySpace +"' order by type asc"
    val schemaDF = Spark.sql(ColumnQuery)
    val schemaArray = schemaDF.collect()
    var selectQuery = "select "
    schemaArray.foreach { row =>

      val map = row.getValuesMap(Seq("column_name", "type"))
      val colname = map.get("column_name").get.toString()
      var coltype = map.get("type").get.toString()

      if (coltype.equalsIgnoreCase("timestamp") || coltype.equalsIgnoreCase("date")) {
        selectQuery = selectQuery + colname +", "
      }else if  (coltype.equalsIgnoreCase("text")) {
        selectQuery = selectQuery + "isString(" + colname + ") as " + colname + ", "
      }else if  (coltype.equalsIgnoreCase("float")) {
        selectQuery = selectQuery + "isFloat(" + colname + ") as " + colname + ", "
      } else if  (coltype.equalsIgnoreCase("double")) {
        selectQuery = selectQuery + "isDouble(" + colname + ") as " + colname + ", "
      }  else if  (coltype.equalsIgnoreCase("int")) {
        selectQuery = selectQuery +  "isInt(" + colname + ") as " + colname + ", "
      }  else if  (coltype.equalsIgnoreCase("bigint")) {
        selectQuery = selectQuery +  "isBigInt(" + colname + ") as " + colname + ", "
      }

    }
    selectQuery = removeLastDelimiter(selectQuery, ",", 1)

    selectQuery
  }

  def loadUtil(spark: SparkSession): Unit = {
    println ("Registering UDF")

    spark.udf.register("isInt", isInt _)
    spark.udf.register("isDouble", isDouble _)
    spark.udf.register("isBigInt", isBigInt _)
    spark.udf.register("isString", isString _)
    spark.udf.register("isFloat", isFloat _)
  }


}