package com.eit.service

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
  * Created by anonymous on 23/06/2017.
  */
class Referential(keySpace: String) extends Serializable{
  val refFiles2DFs: com.eit.tools.csvToDF = new com.eit.tools.csvToDF()

  def loadCellsFromFile(sc:SparkContext,sparkSession: SparkSession,pathInput:String,splitChar:String) = {
    //CELLS
/*    val headerCells = "radio,mcc,net,area,cell,unit,lon,lat,range_v,samples,changeable,created,updated,averagesignal";
    val df_cells_with_dupl = refFiles2DFs.csvToDFusingSqlContext(sc, sparkSession, pathInput, headerCells,splitChar)
    df_cells_with_dupl.repartition(1000).cache().createOrReplaceTempView("CELLS")*/
    sparkSession.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "cell_tower", "keyspace" ->  keySpace)).load().repartition(1000).cache().createOrReplaceTempView("CELLS")


  }

  def loadTacsFromFile(sc:SparkContext,sparkSession: SparkSession,pathInput:String,splitChar:String) = {
    //TACS
   /* val headerTacs = "company,model,tac";
    val df_tacs_with_dupl = refFiles2DFs.csvToDFusingSqlContext(sc, sparkSession, pathInput, headerTacs,splitChar)
    df_tacs_with_dupl.createOrReplaceTempView("TACS_non_unique")
    sparkSession.sql("select distinct * from TACS_non_unique").cache().createOrReplaceTempView("TACS")*/
   val df_tacs_with_dupl = sparkSession.read.format("com.crealytics.spark.excel").option("useHeader", "true").load(pathInput)
    df_tacs_with_dupl.createOrReplaceTempView("TACS_non_unique")
    sparkSession.sql("select distinct * from TACS_non_unique").cache().createOrReplaceTempView("TACS")

  }
  def loadMccmncFromFile(sc:SparkContext,sparkSession: SparkSession ,pathInput:String,splitChar:String) = {
    //MCCMNC
    val headerMCCMNC = "MCC,MCC_INT,MNC,MNC_INT,ISO,COUNTRY,COUNTRY_CODE,NETWORK";
    val df_mccmnc = refFiles2DFs.csvToDFusingSqlContext(sc, sparkSession, pathInput, headerMCCMNC,splitChar)
    df_mccmnc.cache().createOrReplaceTempView("MCCMNC")

  }

  def loadGeoFromFile(sc:SparkContext,sparkSession: SparkSession ,pathInput:String,splitChar:String) = {
    //GEO
    val headerGeo = "GGSN,geosite";
    val df_geo = refFiles2DFs.csvToDFusingSqlContext(sc, sparkSession, pathInput, headerGeo,splitChar)
    df_geo.cache().createOrReplaceTempView("GEO")

  }

  def loadTechnoFromFile(sc:SparkContext,sparkSession: SparkSession ,pathInput:String,splitChar:String) = {
    val headerTechno = "RAT,technology";
    val df_techno = refFiles2DFs.csvToDFusingSqlContext(sc, sparkSession, pathInput, headerTechno,splitChar)
    df_techno.cache().createOrReplaceTempView("TECHNO")
  }


}
