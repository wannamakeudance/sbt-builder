package com.eit.utils

import org.apache.spark.sql.Row

/**
  * Created by anonymous on 03/07/2017.
  */
object Misc {
  def isCorrect (r:Row) : Boolean = {
    val mccmnc = r(307).toString
    val rat = r(306).toString
    val ggsn = r(312).toString
    val IMEI = r(304).toString
    val newVal = mccmnc match{
      case "208-09" => "MNO1"
      case "208-10" => "MNO1"
      case "208-11" => "MNO1"
      case "208-13" => "MNO1"
      case "208-01" => "MNO2"
      case "208-02" => "MNO2"
      case "208-20" => "MNO3"
      case "208-21" => "MNO3"
      case "208-88" => "MNO3"
      case "208-26" => "Roaming"
      case _ => "NULL"
    }
    if(newVal!="NULL" && (rat=="1" || rat=="2" || rat=="6") && ggsn!="\"\"" && ggsn!="\"\"\"\"\"\"" && IMEI!="\"\""){
      return true
    }
    else{
      return false
    }
  }
}
