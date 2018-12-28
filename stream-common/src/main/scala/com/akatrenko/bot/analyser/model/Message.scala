package com.akatrenko.bot.analyser.model

import java.sql.Timestamp

case class Message (unixTime: Long, categoryId: Int, ip: String, actionType: String) extends Serializable

case class MessageEvent(unixTime: Timestamp, categoryId: Int, ip: String, clickEvent: Long, viewEvent: Long)

case class MessageAgg(categories: Set[Int], ip: String, clicks: Long, views: Long) extends Serializable {
  def checkType: Boolean = {
    clicks == 1 || views == 1
  }

  def +(other: MessageAgg): MessageAgg = {
    MessageAgg(categories ++ other.categories, ip, clicks + other.clicks, views + other.views)
  }
}