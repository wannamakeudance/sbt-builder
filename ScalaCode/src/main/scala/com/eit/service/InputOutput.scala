package com.eit.service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

/**
  * Created by anonymous on 12/06/2017.
  */
class InputOutput extends Serializable {
  def addQuotes(s:String) : String = {
    return "'"+ s + "'"
  }

  def rowToString (r:Row) : String = {
    var s: String = ""
    for(i <- 0 to r.size-1){
      if(i!=r.size-1){
        if(r(i)!=null){
          s+=r(i).toString
          s+=","
        }else{
          s+="NULL"
          s+=","
        }
      }else{
        if(r(i)!=null){
          s+=r(i).toString
        }else{
          s+="NULL"
        }
      }

    }
    return s
  }

  def CSV (df:DataFrame,pathOutput:String) = {
    df.write.format("com.databricks.spark.csv").save(pathOutput+"/unaggregated")
  }

  def textFile (df:DataFrame,pathOutput:String) = {
    df.rdd.map(r=>rowToString(r)).saveAsTextFile(pathOutput+"/unaggregated")
  }
/*
  def hiveInsertRawTable (hiveContext:HiveContext,tableRaw:String,env:String ) = {
    println("##################################HIVE####################################")
    if(env=="dev"){
      hiveContext.sql("truncate table "+tableRaw);
    }
    //df_udr.write.mode("append").saveAsTable(tableRaw)

    val queryRawTableInsertion = "insert into "+ tableRaw + " select * from RAW"
    hiveContext.sql(queryRawTableInsertion)
  }
  def hiveInsertFilesTable (hiveContext:HiveContext,tableFiles:String,env:String,fileName:String,epoch:Long,date:String,nbrUniqueUsers:Long,tableOneRow:String ) = {
    println("##################################HIVE####################################")
    if(env=="dev"){
      hiveContext.sql("truncate table "+tableFiles)
    }


    val queryFilesTableInsertion = "insert into "+tableFiles+" select "+ addQuotes(fileName) +","+ addQuotes(epoch.toString) +","+ addQuotes(date) +","+ nbrUniqueUsers +" from (select 1 from "+ tableOneRow + " limit 1) a"
    hiveContext.sql(queryFilesTableInsertion)
  }

  def hiveInsertRawDenormTable (hiveContext:HiveContext,tableFiles:String,env:String,fileName:String,epoch:Long,date:String,nbrUniqueUsers:Long,tableOneRow:String ) = {
    println("##################################HIVE####################################")



    val queryFilesTableInsertion = "insert into "+tableFiles+" select "+ addQuotes(fileName) +","+ addQuotes(epoch.toString) +","+ addQuotes(date) +","+ nbrUniqueUsers +" from (select 1 from "+ tableOneRow + " limit 1) a"
    hiveContext.sql(queryFilesTableInsertion)
  }*/

  def cassandraInsertRawTable(){

  }
  def cassandraInsertFilesTable(){

  }

  def cassandraInsertRawDenormTable(){

  }

  def importGroupsFile(sc:SparkContext,pathGroupList:String,header:String): ArrayBuffer[Tuple3[Int,String,String]] = {
    //RECUPERATION DES INDICES DES COLONNES A PIVOTER
    //chargement du fichier contenant les groupes
    val rdd_groups = sc.textFile(pathGroupList)
    val listGroup = rdd_groups.collect()

    val arrayHeader = header.toUpperCase().split(",")

    var tupleAppsAndGroups = new ArrayBuffer[Tuple3[Int,String,String]]
    for(jj<-0 to arrayHeader.size - 1){
      if((!arrayHeader(jj).contains("VOLUP GLOBAL"))&&(!arrayHeader(jj).contains("VOLDN GLOBAL"))&&(arrayHeader(jj).contains("VOLUP") || arrayHeader(jj).contains("VOLDN")) || arrayHeader(jj).contains("ARTT") || arrayHeader(jj).contains("IRTT") || arrayHeader(jj).contains("RETRATEDN") || arrayHeader(jj).contains("RETRATEUP")){//can be improved
      val arrayOneCol = arrayHeader(jj).split(" ")
        if(listGroup.contains(arrayOneCol(1))){
          tupleAppsAndGroups+=Tuple3(jj,arrayOneCol(0)+" GROUPS",arrayOneCol(1))
        }else{
          tupleAppsAndGroups+=Tuple3(jj,arrayOneCol(0)+" APP",arrayOneCol(1))
        }

      }
    }

    return tupleAppsAndGroups
  }


}
