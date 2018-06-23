package com.eit.tools

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra._

/**
  * Created by anonymous on 26/06/2017.
  */
class DfToCassandra extends Serializable{
  def write(df:DataFrame,keyspace:String,table:String) = {
    df.write.cassandraFormat(keyspace, table).save()
  }
}
