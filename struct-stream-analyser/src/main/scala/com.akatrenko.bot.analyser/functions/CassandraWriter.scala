package com.akatrenko.bot.analyser.functions

import java.util.Properties

import com.akatrenko.bot.analyser.model.BadBot
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.ForeachWriter

class CassandraWriter(connector: CassandraConnector, props: Properties) extends ForeachWriter[BadBot] {
  val cassandraKeySpaceName: String = props.getProperty("sstreaming.cassandra.keyspace.name")
  val cassandraTableName: String = props.getProperty("sstreaming.cassandra.table.name")
  val cassandraTTL: Int = props.getProperty("sstreaming.cassandra.ttl").toInt

  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(record: BadBot): Unit = {
    connector.withSessionDo(session => session.execute(cassandraQuery(record)))
  }

  def close(errorOrNull: Throwable): Unit = {
  }

  def cassandraQuery(record: BadBot): String = {
    println(s"Bad bot: $record")
    s"""INSERT INTO $cassandraKeySpaceName.$cassandraTableName (ip, date_create, source_stream)
       |VALUES('${record.ip}', '${record.date_create}', '${record.source_stream}')
       |USING TTL $cassandraTTL""".stripMargin
  }

}
