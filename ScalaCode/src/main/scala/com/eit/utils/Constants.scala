package com.eit.utils

object Constants {

  val  CASSANDRA_KEYSPACE_PROD : String = "ava_ks_21"
  val  CASSANDRA_KEYSPACE_DEV : String = "ava_ks_21"


  val S3BUCKET_PROD : String  = "s3a://solutions-production"
  val S3BUCKET_DEV : String  = "s3a://solutions-development"

  val S3_ACCESS_KEY_NAME : String = "fs.s3a.access.key"
  val S3_SECRET_KEY_NAME : String = "fs.s3a.secret.key"
  val FILESYSTEM_IMPLEMENTATION_NAME : String = "fs.s3a.impl"
  val ENDPOINT_NAME : String = "fs.s3a.endpoint"
  val SSL_CONNECTION_NAME : String = "fs.s3a.connection.ssl.enabled"
  val FILESYSTEM_IMPLEMENTATION_VALUE : String = "org.apache.hadoop.fs.s3a.S3AFileSystem"
  val S3_ENDPOINT : String = "es-ka-s3-dhn-14.eecloud.nsn-net.net"
  val ACCESS_KEY : String = ""
  val SECRET_KEY : String = ""

  val RECEIVED : String = "/sdaas/sprint/data/received/"
  val PROCEEDED : String = "/sdaas/sprint/data/proceeded/"
  val ERROR : String = "/sdaas/sprint/data/error/"
  val REPORTED : String = "/sdaas/sprint/data/reported/"
  val OBJECT_KEY_REPORTED : String = "sdaas/sprint/data/reported/"


  val TRUE : String = "true"
  val FALSE : String ="false"




}
