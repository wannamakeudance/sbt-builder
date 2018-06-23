package com.eit.tools

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by anonymous on 23/06/2017.
  */
class csvToDF extends Serializable {
  def csvToDFusingSqlContext(sc:SparkContext,sparkSession: SparkSession ,pathInput:String,header:String,splitChar:String):DataFrame={
    val rdd = sc.textFile(pathInput).map(line => Row.fromSeq(line.split(splitChar,-1)))
    val headerMaj = header.toUpperCase();

    val schema =
      StructType(
        headerMaj.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    return sparkSession.createDataFrame(rdd,schema)
    //df_cells_with_dupl.registerTempTable("CELLS_WITH_DUPL")
    //val df_cells = sqlContext.sql("select a.* from CELLS_WITH_DUPL a inner join (select CELL, max(UPDATED) as m from CELLS_WITH_DUPL group by CELL) b on  a.CELL=b.CELL and b.m=a.UPDATED")

  }


}