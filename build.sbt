name := "hbase_poc"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-common" % "2.0.0",
  "org.apache.hbase" % "hbase-client" % "2.0.0",
  "org.apache.hbase" % "hbase-endpoint" % "2.0.0",
  "org.apache.hadoop" % "hadoop-common" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)