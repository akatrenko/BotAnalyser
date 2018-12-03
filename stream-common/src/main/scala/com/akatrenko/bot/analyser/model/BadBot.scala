package com.akatrenko.bot.analyser.model

import java.sql.Timestamp

/**
  * Structure of Cassandra Table
  *
  * @param ip           bot ip address
  * @param rule         rule which define bot
  * @param datecreate   date created
  * @param sourcestream input stream (DStream or Structured Streaming)
  */
case class BadBot(ip: String, rule: String, datecreate: Timestamp, sourcestream: String) extends Serializable
