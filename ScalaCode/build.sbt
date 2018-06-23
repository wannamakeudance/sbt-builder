name := "SDaaS-EIT"

version := "0.1"

scalaVersion := "2.11.8"

organization := ".ava.sdaas-eit"

publishMavenStyle := true

isSnapshot := true

publishTo := {
  val artifactory = "http://artifactory-espoo1.ext.net..com/artifactory/"
  Some("Artifactory Realm" at artifactory +"ava-maven-snapshots-local")
}

credentials += Credentials("Artifactory Realm",  "artifactory-espoo1.ext.net..com", "username",  "password")

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

resolvers ++= Seq(
  "apache-snapshots" at "https://repo1.maven.org/maven2/"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"

libraryDependencies += "com.crealytics" %% "spark-excel" % "0.9.5"


libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.2"  exclude("joda-time", "joda-time")

libraryDependencies ++= Seq( "joda-time" % "joda-time" % "2.3" )






