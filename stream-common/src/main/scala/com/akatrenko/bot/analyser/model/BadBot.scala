package com.akatrenko.bot.analyser.model

import java.sql.Timestamp

/**
  * Structure of Cassandra Table
  *
  * @param ip           bot ip address
  * @param date_create   date created
  * @param source_stream input stream (DStream or Structured Streaming)
  */
case class BadBot(ip: String, date_create: Timestamp, source_stream: String) extends Serializable
