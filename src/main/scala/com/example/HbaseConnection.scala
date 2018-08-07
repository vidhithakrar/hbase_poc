package com.example

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

trait HbaseConnection {
  val conf: Configuration = HBaseConfiguration.create()
  conf.addResource(new Path("/Users/vidhi/Softwares/hbase-2.0.0/conf/hbase-site.xml"))
//  conf.set("hbase.client.retries.number", "3")

  println("trying to create connection...")
  val connection: Connection = ConnectionFactory.createConnection(this.conf)
  println("got connection...")
}
