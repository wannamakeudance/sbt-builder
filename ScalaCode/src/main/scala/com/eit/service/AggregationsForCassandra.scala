package com.eit.service

import com.eit.tools.DfToCassandra
import org.joda.time.LocalDateTime
import com.eit.utils.DateFunctions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import java.util.concurrent.{Callable, ExecutionException, ExecutorService, Executors, FutureTask}

import com.eit.utils.Utilities.dataTypeConversion


/**
  * Created by anonymous on 26/06/2017.
  */
class AggregationsForCassandra(keySpace: String) extends Serializable {
  val dfToCassandra: DfToCassandra = new DfToCassandra()
  //val dayDate = toDay(date)
  //val mondayMidnightDate = FirstDayOfTheWeek(localDateTime)

  def aggregation(session: SparkSession,hour: String, start: Long) = {

    import session.implicits._
    println("Hours.."+hour)
    val end = System.currentTimeMillis()
    println(s"Elapsed time before reading data from ipraw: ${(end - start) / 1000.0} seconds")

    val ip_raw= session.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "ip_raw_denorm", "keyspace" ->  keySpace)).load().filter(($"startdate".substr(0,13)).equalTo(hour))
    ip_raw.createOrReplaceTempView("ip_raw")
   println("count of data for this hour"+ip_raw.count())
    val end1 = System.currentTimeMillis()
    println(s"Elapsed time before reading data from ipraw: ${(end1 - start) / 1000.0} seconds")

    var dfAgg = session.sql (" select apporgroupname,geosite,lon,lat,imsi,rat,mno,model, company,concat(substr(startdate, 0, 16),':00') as startdate,substr(startdate, 0, 10) as pickdates,"+
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then voluptb end) as voluptb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then voldntb end) as voldntb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then valuetb end) as valuetb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then volupgb end) as volupgb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then voldngb end) as voldngb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then valuegb end) as valuegb, " +
      " sum(case when (groupingtype = 'VOL APPS' or groupingtype = 'VOL GROUPS') then value end) as valueb, " +
      " sum(case when (groupingtype = 'VOL APPS') then valuegb end) as valueappsgb, " +
      " sum(case when (groupingtype = 'VOL GROUPS') then valuegb end) as valueservicesgb, " +
      " sum(case when (groupingtype = 'ARTT GROUPS') then case when artt <>0 then artt end  end) sum_artt, " +
      " sum(case when (groupingtype = 'IRTT GROUPS') then case when irtt <>0 then irtt  end  end) as sum_irtt, " +
      " sum(case when (groupingtype = 'RETRATEUP GROUPS') then case when retrateup <>0 then retrateup end  end) as sum_retrateup, " +
      " sum(case when (groupingtype = 'RETRATEDN GROUPS') then  case when retratedn <>0 then retratedn end  end) as sum_retratedn, " +
      " count(case when (groupingtype = 'ARTT GROUPS') then case when artt <>0 then artt end  end) count_artt, " +
      " count(case when (groupingtype = 'IRTT GROUPS') then case when irtt <>0 then irtt  end  end) as count_irtt, " +
      " count(case when (groupingtype = 'RETRATEUP GROUPS') then case when retrateup <>0 then retrateup end  end) as count_retrateup, " +
      " count(case when (groupingtype = 'RETRATEDN GROUPS') then  case when retratedn <>0 then retratedn end  end) as count_retratedn, " +
      " sum(case when ((groupingtype = 'VOL APPS') and techno = '2G') then valuegb end) as valuegbapps2g, " +
      " sum(case when ((groupingtype = 'VOL GROUPS') and techno = '2G') then valuegb end) as valuegbservice2g, " +
      " sum(case when ((groupingtype = 'VOL APPS') and techno = '3G') then valuegb end) as valuegbapps3g, " +
      " sum(case when ((groupingtype = 'VOL GROUPS') and techno = '3G') then valuegb end) as valuegbservice3g, " +
      " sum(case when ((groupingtype = 'VOL APPS') and techno = '4G') then valuegb end) as valuegbapps4g, " +
      " sum(case when ((groupingtype = 'VOL GROUPS') and techno = '4G') then valuegb end) as valuegbservice4g, " +
      "sum(distinct volupglobal) as volupglobaltb, " +
      "sum(distinct voldnglobal) as voldnglobaltb," +
      "(sum(distinct volupglobal) * 1024 ) as volupglobalgb, " +
      "(sum(distinct voldnglobal) * 1024) as voldnglobalgb, "+
      " count(distinct imsi) as uniqueuserscount, " +
      " sum(nbusercnxinc) as uniquesessionscount " +
      " from ip_raw group by apporgroupname,groupingtype,geosite,substr(startdate, 0, 16),substr(startdate, 0, 10),lon,lat,imsi, model, company,rat,mno  Having geosite is not null ")
    dfAgg.cache().createOrReplaceTempView("aggregateTable")


  }



  def allAggregationsHourly(session: SparkSession, hour:String,start :Long) = {

    session.sql("set nbrUniqueUsersHourly=nbrUniqueUsersHourly")
    session.sql("set spark.sql.broadcastTimeout=1000")
    session.sql("set spark.sql.shuffle.partitions = 40")

    aggregation(session: SparkSession,hour: String, start)

    val actionPool: ExecutorService = Executors.newFixedThreadPool(8)
    val action1 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateAppsHourly(session, hour) } })
    val action2 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateDevicesHourly(session, hour) } })
    val action3 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateUsersHourly(session, hour)} })
    val action4 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateLocationsHourly(session,hour)} })
    val action5 = new FutureTask[String](new Callable[String]() { def call(): String = { aggregateCallCenterHourly(session,hour)} })


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

  private def aggregateAppsHourly(session: SparkSession, hour:String ): String= {
    println("Calculating Agg for Apps Horly")

    var dfAggApps = session.sql("select apporgroupname,concat(substr(startdate, 0, 13),':00:00') as startdate,pickdates,rat,mno,geosite,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(valuegbapps2g) as valuegbapps2g,sum(valuegbapps3g) as valuegbapps3g," +
      "sum(valuegbapps4g) as valuegbapps4g,sum(valuegbservice2g) as valuegbservice2g,sum(valuegbservice3g) as valuegbservice3g,sum(valuegbservice4g) as valuegbservice4g" +
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateTable group by apporgroupname,geosite,substr(startdate, 0, 13),pickdates,rat,mno").repartition(1000)
    dfAggApps.createOrReplaceTempView("ip_agg_apps_hourly")
    val selectQuery = dataTypeConversion(session, "ip_agg_apps_hourly", keySpace) + " from ip_agg_apps_hourly"
    dfAggApps = session.sql(selectQuery)
    dfAggApps.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_apps_hourly", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_apps_hourly CREATED IN CASSANDRA "
  }
  private def aggregateDevicesHourly(session: SparkSession, hour:String) : String= {
    println("Calculating Agg for Devices Hourly")

    var df_agg_devices = session.sql("select model,company,concat(substr(startdate, 0, 13),':00:00') as startdate,pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateTable group by  model,company,geosite,substr(startdate, 0, 13),pickdates,rat,mno").repartition(1000)

    df_agg_devices.createOrReplaceTempView("ip_agg_devices_hourly")
    val selectQuery = dataTypeConversion(session, "ip_agg_devices_hourly", keySpace) + " from ip_agg_devices_hourly"
    df_agg_devices = session.sql(selectQuery)

    /*
        df_agg_devices.createOrReplaceTempView("aggregateDeviceHourly")
        df_agg_devices = session.sql("select row_number() over (Order by 1) seq_no, device.* from aggregateDeviceHourly device")
    */

    df_agg_devices.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_devices_hourly", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_devices_hourly CREATED IN CASSANDRA "
  }
  private def aggregateLocationsHourly(session: SparkSession,hour:String) : String= {
    println("Calculating Agg for Location Hourly")

    def RemovenullUDF = udf ((make: String) =>{
      if(make == null || make.trim.isEmpty()) 0.0 else make.toDouble
    })

    var df_agg_locations = session.sql("select lon,lat,concat(substr(startdate, 0, 13),':00:00') as startdate, pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct volupglobaltb) as volupglobaltb,sum(distinct voldnglobaltb) as voldnglobaltb,"+
      "sum(distinct volupglobalgb) as volupglobalgb,sum(distinct voldnglobalgb) as voldnglobalgb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb " +
      "from aggregateTable group by  lon,lat,geosite,substr(startdate, 0, 13),pickdates,rat,mno").repartition(1000)


    df_agg_locations.createOrReplaceTempView("ip_agg_location_hourly")
    val selectQuery = dataTypeConversion(session, "ip_agg_location_hourly", keySpace) + " from ip_agg_location_hourly"
    df_agg_locations = session.sql(selectQuery)

    /*   df_agg_locations.createOrReplaceTempView("aggregateLocationHourly")
       df_agg_locations = session.sql("select row_number() over (Order by 1) seq_no, location.* from aggregateLocationHourly location")
   */
    df_agg_locations = df_agg_locations.withColumn("lon", RemovenullUDF(df_agg_locations("lon")))
    df_agg_locations = df_agg_locations.withColumn("lat", RemovenullUDF(df_agg_locations("lat")))
    print("after udf")



    // df_agg_locations.show(30)
    df_agg_locations.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_location_hourly", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_locations_hourly CREATED IN CASSANDRA "
  }

  private def aggregateUsersHourly(session: SparkSession,hour:String): String = {
    println("Calculating Agg for user Hourly")

    var df_agg_users = session.sql("select imsi,concat(substr(startdate, 0, 13),':00:00') as startdate,pickdates,geosite,rat,mno,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct voldnglobalgb) as voldnglobalgb,sum(distinct voldnglobaltb) as voldnglobaltb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb," +
      "sum(distinct volupglobalgb) as volupglobalgb,sum(distinct volupglobaltb)  as volupglobaltb from aggregateTable group by imsi,geosite,substr(startdate, 0, 13),pickdates,rat,mno").repartition(1000)

    df_agg_users.createOrReplaceTempView("ip_agg_users_hourly")
    val selectQuery = dataTypeConversion(session, "ip_agg_users_hourly", keySpace) + " from ip_agg_users_hourly"
    df_agg_users = session.sql(selectQuery)
    /*
        df_agg_users.createOrReplaceTempView("aggregateUserHourly")
        df_agg_users = session.sql("select row_number() over (Order by 1) seq_no, user.* from aggregateUserHourly user")
    */

    //df_agg_users.show(30)
    df_agg_users.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_users_hourly", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_users_hourly CREATED IN CASSANDRA"
  }
  private def aggregateCallCenterHourly(session: SparkSession,hour:String): String = {
    //Optimiser sans passer par les tables intermédiaires

    val count_table = session.sql("select case when count(distinct apporgroupname)>1 AND count(distinct company) == 1 AND count(distinct model) == 1 then  count(distinct apporgroupname)" +
      " when count(distinct apporgroupname)>1 AND count(distinct company) == 1 AND count(distinct model)>1 then count(distinct apporgroupname)+count(distinct company)+count(distinct model)"+
      "when count(distinct apporgroupname)==1 AND count(distinct company) == 1 AND count(distinct model) == 1 then  count(distinct apporgroupname) end as count, imsi from aggregateTable group by imsi")

    count_table.createOrReplaceTempView("count_table")


    val temp_table = session.sql("select sum(distinct voldnglobalgb)/c.count as voldnglobalgb,sum(distinct voldnglobaltb)/c.count voldnglobaltb,sum(distinct volupglobalgb)/c.count volupglobalgb,sum(distinct volupglobaltb)/c.count volupglobaltb,t.imsi imsi_user from aggregateTable t join count_table c on t.imsi=c.imsi  group by t.imsi,c.count ")
    temp_table.createOrReplaceTempView("temp")



    var df_agg_Centers = session.sql("select apporgroupname,model,company,imsi,concat(substr(startdate, 0, 13),':00:00')  as startdate,pickdates,rat,mno,geosite,sum(sum_artt) as sum_artt," +
      "sum(sum_irtt ) as sum_irtt,sum(sum_retratedn) as sum_retratedn,sum(sum_retrateup) as sum_retrateup,sum(count_artt ) as count_artt,sum(count_irtt) as count_irtt," +
      "sum(count_retratedn) as count_retratedn,sum(count_retrateup) as count_retrateup,sum(uniquesessionscount) as uniquesessionscount,sum(uniqueuserscount) as uniqueuserscount" +
      ",sum(valueappsgb) as valueappsgb ,sum(valueb) as valueb,sum(valuegb) as valuegb,sum(distinct tt.voldnglobalgb) as voldnglobalgb, sum(distinct tt.voldnglobaltb) as voldnglobaltb"+
      ",sum(valueservicesgb) as valueservicesgb,sum(valuetb) as valuetb,sum(voldngb) as voldngb,sum(voldntb) as voldntb,sum(volupgb) as volupgb,sum(voluptb) as voluptb," +
      "sum(distinct tt.volupglobalgb) as volupglobalgb,sum(distinct tt.volupglobaltb)  as volupglobaltb from aggregateTable t join temp tt on t.imsi = tt.imsi_user group by apporgroupname,imsi,model,company,geosite,substr(startdate, 0, 13),pickdates,rat,mno")

    df_agg_Centers.createOrReplaceTempView("ip_agg_call_center_hourly")
    val selectQuery = dataTypeConversion(session, "ip_agg_call_center_hourly", keySpace) + " from ip_agg_call_center_hourly"
    df_agg_Centers = session.sql(selectQuery)
    /* df_agg_Centers.createOrReplaceTempView("aggregateCallCenterHourly")
     df_agg_Centers = session.sql("select row_number() over (Order by 1) seq_no, callcenter.* from aggregateCallCenterHourly callcenter")
 */
    //df_agg_Centers.show(30)
    df_agg_Centers.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_agg_call_center_hourly", "keyspace" -> keySpace)).mode("append").save()
    "ip_agg_call_center_hourly CREATED IN CASSANDRA "
  }

}