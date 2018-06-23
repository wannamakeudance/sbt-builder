package com.eit.service

import java.io.{BufferedReader, InputStreamReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class Config extends Serializable {


  var env = "-"
  var pathInputCells = ""
  var pathInputTacs = ""
  var pathInputTacsMaj =""
  var pathGroupList = ""
  var pathInputMccMnc = ""
  var pathInputGeo = ""
  var pathInputTechno = ""

  var projectName = "-"
  var step:Int = -1
  var subset = "no"
  var tableRaw = "-"
  var tableRawDenorm = "-"
  var tableFiles = "-"
  var tableOneRow = "-"
  var hiveQLJoinClauseSelect = "-"
  var hiveQLJoinClauseJoin = ""
  var hiveQLJoinClauseWhere = ""
  var fileAggs = ""
  var hiveQLAggsByApps = ""
  var hiveQLAggsByDevices = ""
  var hiveQLAggsByUsers = ""
  var hiveQLAggsByLocation = ""
  var hiveQLAggsByCallCenter = ""
  var dbAgg = "-"
  var db = ""



  def loadFromS3(sc:SparkContext, argsFilePath:String): Unit ={

    /*
        /***************** A METTRE DANS LE FICHIER DE CONF**********************/
        val hadoopConf = sc.hadoopConfiguration

        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoopConf.set("fs.s3a.endpoint", "es-ka-s3-dhn-14.eecloud.nsn-net.net")
        hadoopConf.set("fs.s3a.access.key", "secretkey1")
        hadoopConf.set("fs.s3a.secret.key", "secretkey2")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
        /**********************************************************************/

    */

    println(s"config file location $argsFilePath ")
    var rdd = sc.textFile(argsFilePath)
    var tab = rdd.collect()
    //println("nbr of items in config file: "+tab.length)
    for(i <- 0 to (tab.length - 1)){

      var lineTab = tab(i).split(":::")
      var id = lineTab(0)
      //println(id)
      var value = lineTab(1)
      //println(value)
      if(id=="ref_cells"){
        pathInputCells = value
      }
      if(id=="ref_tacs"){
        pathInputTacs = value
      }
      if(id=="ref_groupList"){
        pathGroupList = value
      }
      if(id=="ref_mccmnc"){
        pathInputMccMnc = value
      }
      if(id=="ref_geo"){
        pathInputGeo = value
      }
      if (id == "ref_techno") {
        pathInputTechno = value
      }
      if(id=="step"){
        step = value.toInt
      }
      if(id=="subset"){
        subset = value
        println("subset: "+subset)
      }
      if(id=="projectName"){
        projectName = value
      }
      if(id=="env"){
        env = value
        db = projectName+"_"+env+"."
      }
      if(id=="tableRaw"){
        tableRaw = db+value
        //println("table raw: "+tableRaw)
      }
      if(id=="tableRawDenorm"){
        tableRawDenorm = db+value
        //println("tableRawDenorm: "+tableRawDenorm)
      }
      if(id=="tableFiles"){
        tableFiles = db+value
        //println("tableFiles: "+tableFiles)
      }
      if(id=="tableOneRow"){
        tableOneRow = db+value
        //println("tableOneRow: "+tableOneRow)
      }
      if(id=="hiveQLJoinClauseSelect"){
        hiveQLJoinClauseSelect = value
        //println("hiveQLJoinClauseSelect: "+hiveQLJoinClauseSelect)
      }
      if(id=="hiveQLJoinClauseJoin"){
        hiveQLJoinClauseJoin = hiveQLJoinClauseJoin + " "+ value
        //println("hiveQLJoinClauseJoin: "+hiveQLJoinClauseJoin)
       }
      if(id=="hiveQLJoinClauseWhere"){
        hiveQLJoinClauseWhere = value
        //println("hiveQLJoinClauseWhere: "+hiveQLJoinClauseWhere)
      }
      if(id=="fileAggs"){
        fileAggs = value
        //println("fileAggs: "+fileAggs)
      }

    }
    var rdd2 = sc.textFile(fileAggs)
    var tab2 = rdd.collect()
    for(i <- 0 to (tab2.length - 1)){

      var lineTab2 = tab2(i).split(":::")
      var id = lineTab2(0)
      //println(id)
      var value = lineTab2(1)
      //println(value)



    }
  }



  def loadConfigFromFileSystem(argsFilePath:String):Unit = {
    var pt = new Path(argsFilePath)
    var fs = FileSystem.get(new Configuration())
    var br = new BufferedReader(new InputStreamReader(fs.open(pt)))
    var line = ""
    line = br.readLine()
    while (line != null) {
      var lineTab = line.split(":::")
      var id = lineTab(0)
      var value = lineTab(1)
      println("id: " + id + ", value: " + value)
      if (id == "ref_cells") {
        pathInputCells = value
      }
      if (id == "ref_tacs") {
        pathInputTacs = value
      }
      if (id == "ref_groupList") {
        pathGroupList = value
      }
      if (id == "ref_mccmnc") {
        pathInputMccMnc = value
      }
      if (id == "ref_geo") {
        pathInputGeo = value
      }
      if (id == "ref_techno") {
        pathInputTechno = value
      }
      if (id == "step") {
        step = value.toInt
      }
      if (id == "subset") {
        subset = value
        println("subset: " + subset)
      }
      if (id == "projectName") {
        projectName = value
      }
      if (id == "env") {
        env = value
        db = projectName + "_" + env + "."
      }
      if (id == "tableRaw") {
        tableRaw = db + value
        println("table raw: " + tableRaw)
      }
      if (id == "tableRawDenorm") {
        tableRawDenorm = db + value
        println("tableRawDenorm: " + tableRawDenorm)
      }
      if (id == "tableFiles") {
        tableFiles = db + value
        println("tableFiles: " + tableFiles)
      }
      if (id == "tableOneRow") {
        tableOneRow = db + value
        println("tableOneRow: " + tableOneRow)
      }
      if (id == "hiveQLJoinClauseSelect") {
        hiveQLJoinClauseSelect = value
        println("hiveQLJoinClauseSelect: " + hiveQLJoinClauseSelect)
      }
      if (id == "hiveQLJoinClauseJoin") {
        hiveQLJoinClauseJoin = hiveQLJoinClauseJoin + " " + value
        println("hiveQLJoinClauseJoin: " + hiveQLJoinClauseJoin)
      }
      if (id == "hiveQLJoinClauseWhere") {
        hiveQLJoinClauseWhere = value
        println("hiveQLJoinClauseWhere: " + hiveQLJoinClauseWhere)
      }
      if (id == "fileAggs") {
        fileAggs = value
        println("fileAggs: " + fileAggs)
      }
      line = br.readLine()
    }

    //loadAggQueriesFromFileSystem(fileAggs)

  }

  def loadAggQueriesFromFileSystem(argsFilePath:String):Unit = {
    val pt2=new Path(fileAggs)
    val fs2 = FileSystem.get(new Configuration())
    val br2=new BufferedReader(new InputStreamReader(fs2.open(pt2)))
    var line2 = ""
    line2=br2.readLine()

    var query = ""
    while (line2 != null){
      if(line2==""){
        if(query.contains("table:::agg_apps_hourly ")){//.replace("eit_prod.ip_raw_denorm","RAW_DENORM")
          hiveQLAggsByApps = query.replace("table:::agg_apps_hourly ","").replace(";", "").replace("var_nbrUsers", "\"+nbrUniqueUsersHourly+\"")
        }
        if(query.contains("agg_devices")){
          hiveQLAggsByDevices = query.replace("eit_prod.ip_raw_denorm","RAW_DENORM").replace("eit_prod",projectName+"_"+env).replace(";", "").replace("var_nbrUsers", "\"+nbrUniqueUsersHourly+\"")
        }
        if(query.contains("agg_users")){
          hiveQLAggsByUsers = query.replace("eit_prod.ip_raw_denorm","RAW_DENORM").replace("eit_prod", projectName+"_"+env).replace(";", "").replace("n.nbrUsers", "\"+nbrUniqueUsers+\"")
        }
        if(query.contains("agg_location")){
          hiveQLAggsByLocation = query.replace("eit_prod.ip_raw_denorm","RAW_DENORM").replace("eit_prod", projectName+"_"+env).replace(";", "").replace("n.nbrUsers", "\"+nbrUniqueUsers+\"")
        }
        if(query.contains("agg_call_center")){
          hiveQLAggsByCallCenter = query.replace("eit_prod.ip_raw_denorm","RAW_DENORM").replace("eit_prod", projectName+"_"+env).replace(";", "").replace("n.nbrUsers", "\"+nbrUniqueUsers+\"")
        }

        query = ""
      }
      else{
        query = query + " " + line2
      }
      println(line2)
      line2=br2.readLine()
    }


  }





}
