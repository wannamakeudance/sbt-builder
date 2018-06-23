package com.eit.app

import com.eit.utils.Constants.{CASSANDRA_KEYSPACE_DEV, CASSANDRA_KEYSPACE_PROD, S3BUCKET_DEV, S3BUCKET_PROD}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import com.eit.service.AggregationsForCassandra
import com.eit.service.DailyAggregations
import com.eit.utils.Utilities._
import org.apache.log4j.{Level, Logger}

object DataAggregationApp {


  var sc : SparkContext = null
  var spark : SparkSession = null

  def initCoreAVA(): Unit = {
    spark = SparkSession.builder.appName("SDaaS-EIT")
      .config("spark.cassandra.connection.host", "10.254.112.27,10.254.99.56,10.254.110.216,10.254.105.37,10.157.211.83,10.254.101.50,10.254.113.160,10.254.102.189")
      .config("spark.cassandra.connection.port", "9043")
      .config("spark.cassandra.auth.username", "username")
      .config("spark.cassandra.auth.password", "password").getOrCreate()

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    sc = spark.sparkContext

  }




  def hourly_Aggregation (sparkSession: SparkSession,keySpace: String): Unit = {

    println("starting hourly aggregation..")

    val aggCass = new AggregationsForCassandra(keySpace)

    val df_udr= sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "udr_files", "keyspace" ->  keySpace)).load().cache()
    val df_hourly_status= sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hourly_agg_status", "keyspace" ->  keySpace)).load().cache()

    df_udr.createOrReplaceTempView("udr")
    df_hourly_status.createOrReplaceTempView("status")


    var df_slots = sparkSession.sql("select count(filename) as count,substr(dateutc,0,13) as hour from udr group by substr(dateutc,0,13) order by substr(dateutc,0,13) asc")

    df_slots = df_slots.select("hour").filter("count>=4")
    println("No of hours.."+df_slots.count())

    df_slots.collect().foreach(r=>{
      val hour = r(0).toString
      val df_count = sparkSession.sql("select count(*) as count from status where substr(hour,0,13) == '"+hour+"' and status = 'completed' ")
      if ((df_count.first().getLong(0))==0)
        {
          val start = System.currentTimeMillis()
          println("Starting of hourly agg for.."+hour)
          aggCass.allAggregationsHourly(sparkSession, hour,start)
          val end = System.currentTimeMillis()
          println(s"Elapsed time after hourly aggregation: ${(end - start) / 1000.0} seconds")

          pr_Collect_hourly_status(sparkSession,hour,keySpace)
        }
      else {
        println("already aggregated")
       }

    })

  }

  def daily_Aggregation (sparkSession: SparkSession,keySpace: String): Unit = {
    println("starting Daily aggregation..")

    val aggDaily = new DailyAggregations( keySpace)


    val hourly_agg_status= sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "hourly_agg_status", "keyspace" ->  keySpace)).load().cache()
    val df_daily_status= sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "daily_agg_status", "keyspace" ->  keySpace)).load().cache()

    hourly_agg_status.createOrReplaceTempView("hourly_agg_status")
    df_daily_status.createOrReplaceTempView("status")


    var df_slots = sparkSession.sql("select count(*) as count,day from hourly_agg_status group by day")

    df_slots = df_slots.select("day").filter("count>=24")
    df_slots.show
    println("No of days.."+df_slots.count())

   if (df_slots.count()>0) {
     df_slots.collect().foreach(r => {
       val day = r(0).toString
       val df_count = sparkSession.sql("select count(*) as count from status where day == '" + day + "' and status = 'completed' ")
       if ((df_count.first().getLong(0)) == 0) {
         val start = System.currentTimeMillis()
         println("Starting of dailt agg for.." + day)
         aggDaily.allAggregationsDaily(sparkSession,day)
         val end = System.currentTimeMillis()
         println(s"Elapsed time after daily aggregation: ${(end - start) / 1000.0} seconds")
         pr_Collect_daily_status(sparkSession, day, keySpace)


       }
       else {
         println("already aggregated")
       }

     })
   }

  }

  def mainApp(environment : String ,aggregation: String, sparkSession: SparkSession, sc: SparkContext ): Unit = {


    println("Starting main...")
    var keySpace: String = ""
    var s3Bucket: String = ""

    loadUtil(sparkSession)

    if (environment.equalsIgnoreCase("prod")) {
      keySpace = CASSANDRA_KEYSPACE_PROD
      s3Bucket = S3BUCKET_PROD
    }
    else if (environment.equalsIgnoreCase("dev")) {
      keySpace = CASSANDRA_KEYSPACE_DEV
      s3Bucket = S3BUCKET_DEV
    }


       if(aggregation.equalsIgnoreCase("hourly"))
         {
           hourly_Aggregation(sparkSession,keySpace)
         }
        else if(aggregation.equalsIgnoreCase("daily"))
       {
           daily_Aggregation(sparkSession,keySpace)
       }

  }

  def main(args: Array[String]): Unit = {

    println("Starting main ")

    val aggregation : String = args(0)
    val environment : String = args(1)

    //initCoreAVA()
    //  For running spark submit on  cluster  mode

    initCoreAVA()
    mainApp(environment, aggregation, spark, sc)
    spark.stop()
  }


  }
