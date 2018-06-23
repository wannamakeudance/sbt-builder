package com.eit.service

import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import com.eit.tools.DfToCassandra
import com.eit.utils.DateFunctions._
import com.eit.utils.Utilities.dataTypeConversion
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.joda.time.LocalDateTime


/**
  * Created by anonymous on 26/06/2017.
  */
class DailyAggregations(keySpace: String) extends Serializable {
  val dfToCassandra: DfToCassandra = new DfToCassandra()
  //val dayDate = toDay(date)
  //val mondayMidnightDate = FirstDayOfTheWeek(localDateTime)

  def aggregation(session: SparkSession,day: String) = {

    import session.implicits._
    println("Day.."+day)
    //Reading Hourly Apps
    session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_agg_apps_hourly", "keyspace" ->  keySpace)).load()
           .filter(($"startdate".substr(0,10)).equalTo(day)).cache().createOrReplaceTempView("aggregateAppsTable")

    //Reading Hourly device
    session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_agg_devices_hourly", "keyspace" ->  keySpace)).load()
           .filter(($"startdate".substr(0,10)).equalTo(day)).cache().createOrReplaceTempView("aggregateDeviceTable")

    //Reading Hourly Location
    session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_agg_location_hourly", "keyspace" ->  keySpace)).load()
            .filter(($"startdate".substr(0,10)).equalTo(day)).cache().createOrReplaceTempView("aggregateLocationTable")

    //Reading Hourly user
    session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_agg_users_hourly", "keyspace" ->  keySpace)).load()
            .filter(($"startdate".substr(0,10)).equalTo(day)).cache().createOrReplaceTempView("aggregateUserTable")

    //Reading Hourly Call Center
    session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_agg_call_center_hourly", "keyspace" ->  keySpace)).load()
           .filter(($"startdate".substr(0,10)).equalTo(day)).cache().createOrReplaceTempView("aggregateCallCenterTable")


  }



  def allAggregationsDaily(session: SparkSession, day:String) = {

    session.sql("set spark.sql.broadcastTimeout=1000")
    session.sql("set spark.sql.shuffle.partitions = 40")
    aggregation(session: SparkSession,day: String)

    val actionPool: ExecutorService = Executors.newFixedThreadPool(8)
    val action1 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateAppsDaily(session, day) } })
    val action2 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateDevicesDaily(session,day) } })
    val action3 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateUsersDaily(session,day)} })
    val action4 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateLocationsDaily(session,day)} })
    val action5 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateCallCenterDaily(session,day)} })


    actionPool.execute(action1)
    actionPool.execute(action2)
    actionPool.execute(action3)
    actionPool.execute(action4)
    actionPool.execute(action5)

    print(action1.get)
    print(action2.get)
    print(action3.get)
    print(action4.get)
    print(action5.get)

  }

  private def aggregateAppsDaily(session: SparkSession, day:String ): String= {
    println("Calculating Agg for Apps daily")

    var dfAggApps = session.sql("select apporgroupname,concat((pickdates), ' 00:00:00') as startdate,pickdates,rat,mno,geosite,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(valuegbapps2g) as valuegbapps2g,sum(valuegbapps3g) as valuegbapps3g," +
      "sum(valuegbapps4g) as valuegbapps4g,sum(valuegbservice2g) as valuegbservice2g,sum(valuegbservice3g) as valuegbservice3g,sum(valuegbservice4g) as valuegbservice4g" +
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateAppsTable group by apporgroupname,geosite,pickdates,rat,mno")

    dfAggApps.createOrReplaceTempView("ip_agg_apps_daily")
    val selectQuery = dataTypeConversion(session, "ip_agg_apps_daily", keySpace) + " from ip_agg_apps_daily"
    dfAggApps = session.sql(selectQuery)

    dfAggApps.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_apps_daily", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_apps_daily CREATED IN CASSANDRA "
  }
  private def aggregateDevicesDaily(session: SparkSession, day:String) : String= {
    println("Calculating Agg for Devices daily")

    var df_agg_devices = session.sql("select model,company,concat((pickdates), ' 00:00:00') as startdate,pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateDeviceTable group by  model,company,geosite,pickdates,rat,mno")

    df_agg_devices.createOrReplaceTempView("ip_agg_devices_daily")
    val selectQuery = dataTypeConversion(session, "ip_agg_devices_daily", keySpace) + " from ip_agg_devices_daily"
    df_agg_devices = session.sql(selectQuery)

    df_agg_devices.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_devices_daily", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_devices_daily CREATED IN CASSANDRA "
  }
  private def aggregateLocationsDaily(session: SparkSession,day:String) : String= {
    println("Calculating Agg for Location daily")

    def RemovenullUDF = udf ((make: String) =>{
      if(make == null || make.trim.isEmpty()) 0.0 else make.toDouble
    })

    var df_agg_locations = session.sql("select lon,lat,concat((pickdates), ' 00:00:00') as startdate, pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct volupglobaltb) as volupglobaltb,sum(distinct voldnglobaltb) as voldnglobaltb,"+
      "sum(distinct volupglobalgb) as volupglobalgb,sum(distinct voldnglobalgb) as voldnglobalgb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateLocationTable group by  lon,lat,geosite,pickdates,rat,mno")

    df_agg_locations.createOrReplaceTempView("ip_agg_location_daily")
    val selectQuery = dataTypeConversion(session, "ip_agg_location_daily", keySpace) + " from ip_agg_location_daily"
    df_agg_locations = session.sql(selectQuery)

    /*   df_agg_locations.createOrReplaceTempView("aggregateLocationHourly")
       df_agg_locations = session.sql("select row_number() over (Order by 1) seq_no, location.* from aggregateLocationHourly location")
   */
    df_agg_locations = df_agg_locations.withColumn("lon", RemovenullUDF(df_agg_locations("lon")))
    df_agg_locations = df_agg_locations.withColumn("lat", RemovenullUDF(df_agg_locations("lat")))
    print("after udf")
    // df_agg_locations.show(30)
    df_agg_locations.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_location_daily", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_locations_daily CREATED IN CASSANDRA "
  }

  private def aggregateUsersDaily(session: SparkSession,day:String): String = {
    println("Calculating Agg for user daily")

    var df_agg_users = session.sql("select imsi,concat((pickdates), ' 00:00:00') as startdate,pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct voldnglobalgb) as voldnglobalgb,sum(distinct voldnglobaltb) as voldnglobaltb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb," +
      "sum(distinct volupglobalgb) as volupglobalgb,sum(distinct volupglobaltb)  as volupglobaltb from aggregateUserTable group by imsi,geosite,pickdates,rat,mno")

    df_agg_users.createOrReplaceTempView("ip_agg_users_daily")
    val selectQuery = dataTypeConversion(session, "ip_agg_users_daily", keySpace) + " from ip_agg_users_daily"
    df_agg_users = session.sql(selectQuery)

    /*
        df_agg_users.createOrReplaceTempView("aggregateUserHourly")
        df_agg_users = session.sql("select row_number() over (Order by 1) seq_no, user.* from aggregateUserHourly user")
    */

    //df_agg_users.show(30)
    df_agg_users.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_users_daily", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_users_daily CREATED IN CASSANDRA"
  }
  private def aggregateCallCenterDaily(session: SparkSession,hour:String): String = {
    //Optimiser sans passer par les tables intermédiaires
    println("Calculating Agg for Call Center daily")

    var df_agg_Centers = session.sql("select apporgroupname,imsi,model,company,concat((pickdates), ' 00:00:00') as startdate,pickdates,rat,mno,geosite,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct voldnglobalgb) as voldnglobalgb,sum(distinct voldnglobaltb) as voldnglobaltb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb," +
      "sum(distinct volupglobalgb) as volupglobalgb,sum(distinct volupglobaltb)  as volupglobaltb from aggregateCallCenterTable group by apporgroupname,imsi,model,company,geosite,pickdates,rat,mno")

    df_agg_Centers.createOrReplaceTempView("ip_agg_call_center_daily")
    val selectQuery = dataTypeConversion(session, "ip_agg_call_center_daily", keySpace) + " from ip_agg_call_center_daily"
    df_agg_Centers = session.sql(selectQuery)
    /* df_agg_Centers.createOrReplaceTempView("aggregateCallCenterHourly")
     df_agg_Centers = session.sql("select row_number() over (Order by 1) seq_no, callcenter.* from aggregateCallCenterHourly callcenter")
 */
    //df_agg_Centers.show(30)
    df_agg_Centers.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_call_center_daily", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_call_center_daily CREATED IN CASSANDRA "
  }

}