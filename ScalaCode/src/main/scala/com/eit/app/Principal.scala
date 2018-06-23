package com.eit.app

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, Logger}
import java.net.URI

import org.apache.hadoop.fs._
import java.util.Calendar

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.eit.utils.Constants._
import com.eit.utils.DateFunctions._
import com.eit.service._
import com.eit.utils.Misc._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.eit.utils.Utilities._

object Principal {


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

  def mainApp(pathInputFolder: String, configLocation : String , environment : String , sparkSession: SparkSession, sc: SparkContext ): Unit = {

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

    val hadoopConf = sc.hadoopConfiguration
    // ****** setup S3 connection
    hadoopConf.set(FILESYSTEM_IMPLEMENTATION_NAME, FILESYSTEM_IMPLEMENTATION_VALUE)
    hadoopConf.set(ENDPOINT_NAME, S3_ENDPOINT)
    hadoopConf.set(S3_ACCESS_KEY_NAME, ACCESS_KEY)
    hadoopConf.set(S3_SECRET_KEY_NAME, SECRET_KEY)
    hadoopConf.set(SSL_CONNECTION_NAME, FALSE)
    val s3FileSystem = FileSystem.get(new URI(s3Bucket), sc.hadoopConfiguration) // global variable for S3 file sytem, from here the directory structure starts
    var file_Location = ""
    var fileName = ""
    val filelist = s3FileSystem.listStatus(new Path(pathInputFolder))

    filelist.foreach(  x=> {
      if(x.isFile) {
        val start = System.currentTimeMillis()
        file_Location = x.getPath().toString
        fileName = file_Location.split("/").last
        println("File path===>" + file_Location)
        println("File Name===>" + fileName)
        processRAWFiles(file_Location, fileName, configLocation, sparkSession, sc, keySpace, start)
        pr_move_File(sc, s3FileSystem, fileName, pathInputFolder, "/sdaas/eit/data/proceeded/")
      }

    } )

  }

  def processRAWFiles(pathInputFolder: String, fileName: String, configLocation : String , sparkSession: SparkSession, sc: SparkContext, keySpace: String , start: Long): Unit = {

    println("Inside Process Raw Files.........")
    val config = new Config()
    config.loadFromS3(sc, configLocation)

    var pathInput = pathInputFolder  //spss/testValueTB/EITELECOM_3realrows.csv /spss/UDR/UDR_201701050430_00_000_0015_001151092.csv.gz
    println(" *************************************************************************** ")
    println("File to process: " + pathInput)
    println("File Name: " + fileName)
    println(" *************************************************************************** ")

    var cal: Calendar = Calendar.getInstance()
    var rdd_eitelecom: RDD[String] = null
    if (config.env == "prod") {
      rdd_eitelecom = sc.textFile(pathInput)
    } else {
      if (config.subset == "no") {
        rdd_eitelecom = sc.textFile(pathInput)
      } else {
        //rdd_eitelecom = sc.textFile(pathInput).sample(false, 0.5, System.currentTimeMillis().toInt).cache();
        rdd_eitelecom = sc.textFile(pathInput)
      }
    }

    println("after reading a file")
    val header = rdd_eitelecom.first()

    val rdd_eitelecom_without_header = rdd_eitelecom.filter(line => line != header)

    val rdd_rows_eitelecom = rdd_eitelecom_without_header.map(line => Row.fromSeq(line.split(",")))
    println("Nombre de partitions:" + rdd_rows_eitelecom.partitions.length)

    val inOut = new com.eit.service.InputOutput()

    val epoch = rdd_rows_eitelecom.map(r => r(2)).first().toString.toLong
    val localDateTime = convertDate(epoch, cal)
    val date = localDateTimeToString(localDateTime)

    val arrayTuple = inOut.importGroupsFile(sc, config.pathGroupList, header)

    //UNPIVOTING
    var rdd_res2: RDD[Row] = null

    if (config.step >= 1) {
      println("step===========>1")

      val p = new Pivot()
      println("count in file" + rdd_rows_eitelecom.count())
      rdd_res2 = rdd_rows_eitelecom.repartition(1000).flatMap(r => p.pivotRow(r, date, arrayTuple))
      val end = System.currentTimeMillis()
      println(s"Elapsed time after step 1: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,1,"After reading and transform",(end - start) / 1000.0 + " seconds",keySpace)

    }
    //RDD(UNPIVOTED) TO DF
    var df_udr: DataFrame = null
    var df_udrfiles: DataFrame = null
    if (config.step >= 2) {
      println("step===========>2")

      val headerUDR = "DDate,Type,StartDate,Duration,NbUserCnx,NbUserCnxInc,NbUserErrCnx,VolUpglobal,VolDnglobal,IMEI,IMSI,RAT,MCCMNC,APN,CELLID,LOCID,IPAddr,GGSN,GroupingType,AppOrGroupName,VOLUPTB,VOLDNTB,ValueTB,VOLUPGB,VOLDNGB,ValueGB,Value,ARTT,IRTT,RETRATEUP,RETRATEDN"; //rdd_eitelecom.first();30
      val headerUDRMaj = headerUDR.toLowerCase
      val schema =
        StructType(
          headerUDRMaj.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      //creation du df
      df_udr = sparkSession.createDataFrame(rdd_res2, schema)
      df_udr.coalesce(10).createOrReplaceTempView("RAW")
      val end = System.currentTimeMillis()
      println(s"Elapsed time after step 2: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,2,"After validation and transform into Dataframe",(end - start) / 1000.0 + " seconds",keySpace)

    }
    //INSERT IN IP TABLE
    if (config.step >= 3) {
      println("step===========>3")

      println("count of df_udr====>" + df_udr.count())

      var dfUdrOrdered : DataFrame = null

      val selectQuery = dataTypeConversion(sparkSession, "ip", keySpace) + " from RAW"

      //println("selectQuery------>"+selectQuery)

      dfUdrOrdered = sparkSession.sql(selectQuery).repartition(1000)
      dfUdrOrdered.coalesce(10).cache()
      println("count of df_udr====>" + dfUdrOrdered.count())

      //dfUdrOrdered.repartition(30).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip", "keyspace" -> keySpace)).mode("append").save()
      dfUdrOrdered.createOrReplaceTempView("RAW")
      val end = System.currentTimeMillis()
      println(s"Elapsed time after step 3: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,3,"After Datatype validation",(end - start) / 1000.0 + " seconds",keySpace)

    }
    //INSERT IN FILES TABLE
    var nbrUniqueUsers = 0l
    if (config.step >= 4) {
      println("step===========>4")
      nbrUniqueUsers = rdd_rows_eitelecom.filter(r => isCorrect(r)).map(a => a(305)).distinct().count() //distinct is expensive (30s)
      val end = System.currentTimeMillis()
      print("unique user" + nbrUniqueUsers)
      println(s"Elapsed time after step 4: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,4,"Retrive unique user count",(end - start) / 1000.0 + " seconds",keySpace)

    }
    if (config.step >= 5) {
      println("step===========>5")
      println("**********")
      println("StartDate :")
      println(date.substring(0, 16))
      import sparkSession.implicits._
      val collection = Seq((fileName, epoch, date, nbrUniqueUsers))
      val logDF = collection.toDF("filename", "dateepoch", "dateutc", "nbrusers")
      logDF.show()
      val end = System.currentTimeMillis()
      println(s"Elapsed time after step 5: ${(end - start) / 1000.0} seconds")
      logDF.repartition(1).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "udr_files", "keyspace" -> keySpace)).mode("append").save()
      val end1 = System.currentTimeMillis()
      println(s"Elapsed time after step 5: ${(end1 - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,5,"After writing logs to cassandra",(end1 - start) / 1000.0 + " seconds",keySpace)

    }
    //REFERENTIAL FILES TO DF
    if (config.step >= 6) {
      println("step===========>6")
      val ref: Referential = new Referential(keySpace)
      ref.loadCellsFromFile(sc, sparkSession, config.pathInputCells, ",")
      ref.loadTacsFromFile(sc, sparkSession, config.pathInputTacs, ",")
      ref.loadMccmncFromFile(sc, sparkSession, config.pathInputMccMnc, ";")
      ref.loadGeoFromFile(sc, sparkSession, config.pathInputGeo, ",")
      ref.loadTechnoFromFile(sc, sparkSession, config.pathInputTechno, ",")
      val end = System.currentTimeMillis()
      println(s"Elapsed time after step 6: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,6,"After Reading referential data ",(end - start) / 1000.0 + " seconds",keySpace)

      //TEST
      //sparkSession.sql("select * from CELLS").show(30)
      //sparkSession.sql("select * from TACS").show(30)
      //sparkSession.sql("select * from MCCMNC").show(30)
      // sparkSession.sql("select * from GEO").show(30)
      //sparkSession.sql("select * from TECHNO").show(30)*/
    }
    //INSERT IN DENORM RAW TABLE
    if (config.step >= 7) {
      println("Step ======> 7")
      val join = "left join CELLS c on u.CELLID = c.CELL "
      val sql_txt = config.hiveQLJoinClauseSelect + " " + config.hiveQLJoinClauseJoin + " " + config.hiveQLJoinClauseWhere
      //val sql_txt = select + " " + join
      var df_join: DataFrame = sparkSession.sql(sql_txt).repartition(1000)
      df_join.createOrReplaceTempView("ip_raw_denorm")
      println("count of df_join====>" + df_join.count())
      val end1 = System.currentTimeMillis()
      println(s"Elapsed time before cassandra write @ 7: ${(end1 - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,7,"After joining Raw data with referential data ",(end1 - start) / 1000.0 + " seconds",keySpace)
      df_join = sparkSession.sql("select row_number() over (Order by 1) seq_no, ip_raw_denorm.* from ip_raw_denorm ip_raw_denorm")
      df_join.createOrReplaceTempView("ip_raw_denorm_table")
      val selectQuery = dataTypeConversion(sparkSession, "ip_raw_denorm", keySpace) + " from ip_raw_denorm_table"
      df_join = sparkSession.sql(selectQuery)
      df_join.repartition(1000).write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_raw_denorm", "keyspace" -> keySpace)).mode("append").save()
      sparkSession.catalog.dropTempView("RAW")
      sparkSession.catalog.dropTempView("CELLS")
      sparkSession.catalog.clearCache()
      val end = System.currentTimeMillis()
      println(s"Elapsed time after cassandra write @ 7: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,8,"After Writing IP Raw data to Cassandra ",(end - start) / 1000.0 + " seconds",keySpace)


    }

 /*   val aggCass = new AggregationsForCassandra(date, localDateTime, keySpace)

    if (config.step >= 8) {
      println("Step ======> 8")

      var nbrUniqueUsersHourly = -1l
      //########################################### AGGREGATIONS ###########################################
      val hour = date.substring(0, 13)
      val rdd_file_number = sparkSession.sparkContext.cassandraTable(keySpace, "udr_files").filter(line => line.getString("dateutc").substring(0, 13) == hour) //filter(r=>r(2))//where("dateutc","")
      println("**********Count Hourly Files *******")
      println(rdd_file_number.count())

      /*  import sparkSession.implicits._

        nbrUniqueUsersHourly = sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "ip_raw_denorm", "keyspace" -> keySpace))
          .load().filter(($"startdate".substr(0, 13)).equalTo(hour))
          .agg(countDistinct("imsi").alias("count")).collect()(0).getLong(0)*/

      println("nbrUniqueUsersHourly count" + nbrUniqueUsersHourly)
      println("file list corresponding to the hour:")
      println(hour + ":00:00")
      aggCass.allAggregationsHourly(sparkSession, config, nbrUniqueUsersHourly, hour)

      val end = System.currentTimeMillis()
      println(s"Elapsed time after cassandra write @ 8: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,9,"After Completing Hourly Aggregation ",(end - start) / 1000.0 + " seconds",keySpace)



    }
    val aggDaily = new DailyAggregations(date, localDateTime, keySpace)

    if (config.step >= 9) {
      println("Step ======> 9")

      var nbrUniqueUsersDaily = -1l
      //########################################### AGGREGATIONS ###########################################
      val day = date.substring(0, 10)
      val rdd_file_number = sparkSession.sparkContext.cassandraTable(keySpace, "udr_files").filter(line => line.getString("dateutc").substring(0, 10) == day) //filter(r=>r(2))//where("dateutc","")
      println("**********Count Daily Files *******")
      println(rdd_file_number.count())
      //if (rdd_file_number.count() == 96) {
        //if an hour is completed (4 files processed)
        //retrieve data from ip_raw_denorm
       // val table_tmp = sparkSession.sparkContext.cassandraTable(keySpace, "ip_raw_denorm").filter(line => line.getString("startdate").substring(0, 10) == day)
        //nbrUniqueUsersDaily = table_tmp.map(line => line.getString("imsi")).distinct().count()
        println("file list corresponding to the day:")
        println(day)
        //rdd_file_number.collect().foreach(println)
        aggDaily.allAggregationsDaily(sparkSession, config, nbrUniqueUsersDaily, day)
      val end = System.currentTimeMillis()
      println(s"Elapsed time after cassandra write @ 8: ${(end - start) / 1000.0} seconds")
      pr_Collect_Log(sc,sparkSession,fileName,10,"After Completing Hourly Aggregation ",(end - start) / 1000.0 + " seconds",keySpace)



      // }

    }*/
  }


  def main(args: Array[String]): Unit = {

    println("Starting main ")

    val pathInputFolder : String = args(0)
    val configLocation : String = args(1)
    val environment : String = args(2)

    //initCoreAVA()
    //  For running spark submit on  cluster  mode

    initCoreAVA()

    mainApp(pathInputFolder, configLocation, environment, spark, sc)
    spark.stop()
  }
}
