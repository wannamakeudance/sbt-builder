package com.eit.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}




object ReferentialLoad {

  var sc : SparkContext = null
  var spark : SparkSession = null

  def initCoreAVA(): Unit = {
    spark = SparkSession.builder.appName("Don't Kill")
     /* .config("spark.cassandra.connection.host", "10.131.5.7")
      .config("spark.cassandra.connection.port", "9043")
*/    .config("spark.cassandra.connection.host", "10.157.211.83")
      .config("spark.cassandra.connection.port", "9043")
      .config("spark.cassandra.auth.username", "username")
      .config("spark.cassandra.auth.password", "password").getOrCreate()
       spark.conf.set("spark.cassandra.connection.host", "10.157.211.83")
        spark.conf.set("spark.cassandra.connection.port", "9043")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "endpoint")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "secretkey1")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "secretkey2")
    sc.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")


  }

  def csvToDFusingSqlContext(sc:SparkContext,sparkSession: SparkSession,pathInput:String,headerCells:String,splitChar:String):DataFrame={
    val rdd_file = sc.textFile(pathInput)
    val header = rdd_file.first()
    val rdd_without_header = rdd_file.filter(line => line != header)
    val rdd = rdd_without_header.map(line => Row.fromSeq(line.split(splitChar,-1)))
    val schema =
      StructType(
        headerCells.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    return spark.createDataFrame(rdd,schema)

  }

  def loadCellsFromFile(sc:SparkContext,spark: SparkSession) = {
    //CELLS
    val headerCells = "radio,mcc,net,area,cell,unit,lon,lat,range_v,samples,changeable,created,updated,averagesignal";
    val df_cells_with_dupl = csvToDFusingSqlContext(sc,spark,"s3a://solutions-development/sdaas/eit/data/referential/cell_towers-new.csv.gz", headerCells,",")
    df_cells_with_dupl.dropDuplicates()
    println("count from file"+df_cells_with_dupl.count())
    df_cells_with_dupl.write.save()
    println("completed writing..")
    val df= spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "cell_tower", "keyspace" ->  "ava_ks_21")).load()
    println("count from table"+df.count())


  }

  def main(args: Array[String]): Unit = {

    println("Starting referential main ")

    initCoreAVA()
    loadCellsFromFile(sc,spark)


  }
}
