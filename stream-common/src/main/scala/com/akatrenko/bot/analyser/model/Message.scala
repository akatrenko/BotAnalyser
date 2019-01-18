package com.akatrenko.bot.analyser.model

import java.sql.Timestamp

import org.apache.spark.streaming.Minutes

case class Message (unixTime: Long, categoryId: Int, ip: String, actionType: String) extends Serializable

case class MessageEvent(unixTime: Timestamp, categoryId: Int, ip: String, clickEvent: Long, viewEvent: Long)

case class MessageAgg(categories: Set[Int], ip: String, clicks: Long, views: Long, eventTime: Option[Timestamp] = None,
                      wind: Option[(Timestamp, Timestamp)] = None) extends Serializable {
  def checkType: Boolean = {
    clicks == 1 || views == 1
  }

  def +(other: MessageAgg)(durationInt: Int): MessageAgg = {
    val startTime = eventTime.get
    val endTime = new Timestamp(eventTime.get.getTime + Minutes(durationInt).milliseconds)
    MessageAgg(categories ++ other.categories, ip, clicks + other.clicks, views + other.views,
      eventTime.flatMap(t => other.eventTime.map(o => new Timestamp(math.max(t.getTime, o.getTime)))),
      Some((startTime, endTime)))
  }
}