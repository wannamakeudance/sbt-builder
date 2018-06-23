package com.eit.service

import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

class Pivot extends Serializable{

  def pivotRow (r:Row, newDate:String,tupleAppsAndGroups:ArrayBuffer[Tuple3[Int,String,String]]) : ListBuffer[Row] = {
    var list_rows = new ListBuffer[Row]()
    if(r.size==313){
      //var cal:Calendar  = Calendar.getInstance();

      var volUpGlobal = r(6).toString.toDouble/(1099511627776l)
      var volDnGlobal = r(7).toString.toDouble/(1099511627776l)
      var IMEI = r(304).toString

      for( i <- 0 to tupleAppsAndGroups.size - 1){
        //if(r(tupleAppsAndGroups(i)._1)!="0"){

        var nbusercnx = r(4).toString
        var nbusercnxIncreased = r(4).toString.toInt + 1
        var ggsn = r(312).toString
        var groupingType = tupleAppsAndGroups(i)._2

        var mccmnc = r(307).toString
        var rat = r(306).toString

        if(tupleAppsAndGroups(i)._2 == "ARTT GROUPS"){
          val VOLUPTB = ""
          val VOLDNTB = ""
          var VOLUPGB = ""
          val VOLDNGB = ""
          var VOLUPDNTB = r(tupleAppsAndGroups(i)._1).toString
          var VOLTOTGB = r(tupleAppsAndGroups(i)._1).toString.toDouble
          var VOLUPDNBYTE = r(tupleAppsAndGroups(i)._1).toString.toDouble
          var ARTT = VOLUPDNTB
          var IRTT = "0"
          var RETRATEUP = "0"
          var RETRATEDN = "0"
          if(VOLUPDNTB!="0"){
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,groupingType,tupleAppsAndGroups(i)._3,VOLUPTB,VOLDNTB,VOLUPDNTB,VOLUPGB,VOLDNGB,VOLTOTGB.toString,VOLUPDNBYTE.toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }
        }
        if(tupleAppsAndGroups(i)._2 == "IRTT GROUPS"){
          val VOLUPTB = ""
          val VOLDNTB = ""
          var VOLUPGB = ""
          val VOLDNGB = ""
          var VOLUPDNTB = r(tupleAppsAndGroups(i)._1).toString
          var VOLTOTGB = r(tupleAppsAndGroups(i)._1).toString.toDouble
          var VOLUPDNBYTE = r(tupleAppsAndGroups(i)._1).toString.toDouble
          var ARTT = "0"
          var IRTT = VOLUPDNTB
          var RETRATEUP = "0"
          var RETRATEDN = "0"
          if(VOLUPDNTB!="0"){
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,groupingType,tupleAppsAndGroups(i)._3,VOLUPTB,VOLDNTB,VOLUPDNTB,VOLUPGB,VOLDNGB,VOLTOTGB.toString,VOLUPDNBYTE.toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }

        }
        if(tupleAppsAndGroups(i)._2 == "RETRATEUP GROUPS"){
          var VOLUP = r(tupleAppsAndGroups(i)._1).toString
          var VOLDN = r(tupleAppsAndGroups(i+1)._1).toString
          var VOLUPdouble = VOLUP.toDouble
          var VOLDNdouble = VOLDN.toDouble

          val VOLTOT = VOLUPdouble+VOLDNdouble
          var ARTT = "0"
          var IRTT = "0"
          var RETRATEUP = VOLUP
          var RETRATEDN = VOLDN
          if(VOLUP!="0" || VOLDN!="0"){
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,"RETRATEUP GROUPS",tupleAppsAndGroups(i)._3,VOLUP,VOLDN,VOLTOT.toString,VOLUP,VOLDN,VOLTOT.toString,VOLTOT.toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }

        }
        if(tupleAppsAndGroups(i)._2 == "RETRATEDN GROUPS"){
          var VOLDN = r(tupleAppsAndGroups(i)._1).toString
          var VOLUP = r(tupleAppsAndGroups(i-1)._1).toString
          var VOLUPdouble = VOLUP.toDouble
          var VOLDNdouble = VOLDN.toDouble

          val VOLTOT = VOLUPdouble+VOLDNdouble
          var ARTT = "0"
          var IRTT = "0"
          var RETRATEUP = VOLUP
          var RETRATEDN = VOLDN
          if(VOLUP!="0" || VOLDN!="0"){
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,"RETRATEDN GROUPS",tupleAppsAndGroups(i)._3,VOLUP,VOLDN,VOLTOT.toString,VOLUP,VOLDN,VOLTOT.toString,VOLTOT.toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }
        }

        if(tupleAppsAndGroups(i)._2 == "VOLUP APP"){

          var VOLUPBYTE_string = r(tupleAppsAndGroups(i)._1).toString
          var VOLDNBYTE_string = r(tupleAppsAndGroups(i+1)._1).toString

          if(VOLUPBYTE_string!="0" || VOLDNBYTE_string!="0"){
            var VOLUPBYTE = VOLUPBYTE_string.toDouble
            var VOLDNBYTE = VOLDNBYTE_string.toDouble
            var VOLUPGB = VOLUPBYTE_string.toDouble/(1073741824l)
            var VOLDNGB = VOLDNBYTE_string.toDouble/(1073741824l)
            var VOLTOTGB = (VOLUPGB+VOLDNGB).toString
            var VOLUPTB = VOLUPGB/(1024l)
            var VOLDNTB = VOLDNGB/(1024l)
            var ARTT = "0"
            var IRTT = "0"
            var RETRATEUP = "0"
            var RETRATEDN = "0"
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,"VOL APPS",tupleAppsAndGroups(i)._3,VOLUPTB.toString,VOLDNTB.toString,(VOLUPTB+VOLDNTB).toString,VOLUPGB.toString,VOLDNGB.toString,VOLTOTGB.toString,(VOLUPBYTE+VOLDNBYTE).toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }

        }

        if(tupleAppsAndGroups(i)._2 == "VOLUP GROUPS"){

          var VOLUPBYTE_string = r(tupleAppsAndGroups(i)._1).toString
          var VOLDNBYTE_string = r(tupleAppsAndGroups(i+1)._1).toString

          if(VOLUPBYTE_string!="0" || VOLDNBYTE_string!="0"){
            var VOLUPBYTE = VOLUPBYTE_string.toDouble
            var VOLDNBYTE = VOLDNBYTE_string.toDouble
            var VOLUPGB = VOLUPBYTE_string.toDouble/(1073741824l)
            var VOLDNGB = VOLDNBYTE_string.toDouble/(1073741824l)
            var VOLTOTGB = (VOLUPGB+VOLDNGB).toString
            var VOLUPTB = VOLUPGB/(1024l)
            var VOLDNTB = VOLDNGB/(1024l)
            var ARTT = "0"
            var IRTT = "0"
            var RETRATEUP = "0"
            var RETRATEDN = "0"
            list_rows+=Row(r(0).toString,r(1).toString,newDate,r(3).toString,nbusercnx,nbusercnxIncreased.toString,r(5).toString,volUpGlobal.toString,volDnGlobal.toString,r(304).toString,r(305).toString,r(306).toString,mccmnc,r(308).toString,r(309).toString,r(310).toString,r(311).toString,ggsn,"VOL GROUPS",tupleAppsAndGroups(i)._3,VOLUPTB.toString,VOLDNTB.toString,(VOLUPTB+VOLDNTB).toString,VOLUPGB.toString,VOLDNGB.toString,VOLTOTGB,(VOLUPBYTE+VOLDNBYTE).toString,ARTT,IRTT,RETRATEUP,RETRATEDN)
          }

        }


      }
    }
    return list_rows
  }
}
