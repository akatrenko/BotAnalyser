package com.akatrenko.bot.analyser.functions

import java.util.Properties

import com.akatrenko.bot.analyser.model.{BadBot, Message}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

class CassandraWriter(connector: CassandraConnector, props: Properties) extends ForeachWriter[BadBot] {
  val cassandraKeySpaceName: String = props.getProperty("dstreaming.cassandra.keyspace.name")
  val cassandraTableName: String = props.getProperty("dstreaming.cassandra.table.name")
  val cassandraTTL: Int = props.getProperty("dstreaming.cassandra.ttl").toInt

  def open(partitionId: Long, version: Long): Boolean = {
    // open connection
    true
  }

  def process(record: BadBot): Unit = {
    // write string to connection
    connector.withSessionDo(session => session.execute(cassandraQuery(record)))
  }

  def close(errorOrNull: Throwable): Unit = {
    // close the connection
  }

  def cassandraQuery(record: BadBot): String = {
    println(s"Bad bot: $record")
    s"""INSERT INTO $cassandraKeySpaceName.$cassandraTableName (ip, datecreate, rule, sourcestream)
       |VALUES('${record.ip}', '${record.datecreate}', '${record.rule}', '${record.sourcestream}')
       |USING TTL $cassandraTTL""".stripMargin
  }

}
