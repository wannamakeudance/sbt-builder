package com.eit.utils

import java.text.SimpleDateFormat
import org.joda.time.LocalDateTime
import java.util.Calendar
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import java.util.TimeZone
import org.joda.time.DateTimeConstants

object DateFunctions extends Serializable {

  def convertDate(epoch:Long,cal:Calendar) : LocalDateTime = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    cal.setTimeInMillis(epoch);
    val newDate = formatter.format(cal.getTime());
    //println("startdate: "+newDate)

    val tz = TimeZone.getDefault();

    //println("TimeZone : " + tz.getID() + " - " + tz.getDisplayName());
    //System.out.println("Date : " + formatter.format(cal.getTime()));

    val dateStringFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    val dt  = dateStringFormat.parseDateTime(newDate);


    //val dt = new DateTime(newDate)

    //new dateTimeZone
    val dtZone = DateTimeZone.forID("UTC");
    //new dateTimeZone applied on the dateTime
    val dtus = dt.withZone(dtZone);

    //conversion de la dateTimeZone en timezone
    val universalTZ = dtZone.toTimeZone();

    return dtus.plusSeconds(1).toLocalDateTime()
  }

  def localDateTimeToString(localDateTime:LocalDateTime) : String = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    formatter.format(localDateTime.toDate())
  }

  def toDay(theDate:String):String = {
    var newDate1 = new SimpleDateFormat("yyyy-MM-dd").parse(theDate.substring(0, 10));
    return new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(newDate1);
  }

  def FirstDayOfTheWeek(localDateTime:LocalDateTime):String = {

    val mondayDate = localDateTimeToString(localDateTime.withDayOfWeek(DateTimeConstants.MONDAY))
    var mondayMidnightDate = new SimpleDateFormat("yyyy-MM-dd").parse(mondayDate.substring(0, 10));
    return new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(mondayMidnightDate);
  }
}
