package com.akatrenko.bot.analyser.model

import java.sql.Timestamp

import com.akatrenko.bot.analyser.constant.MessageType

case class Message (unixTime: String, categoryId: Int, ip: String, actionType: String) extends Serializable {
  def checkType: Boolean = {
    actionType == MessageType.ClickType || actionType == MessageType.ViewType
  }
}

case class MessageEvent(unixTime: Timestamp, categoryId: Int, ip: String, clickEvent: Long, viewEvent: Long)

case class MessageAgg(categories: Set[Int], ip: String, clicks: Long, views: Long) extends Serializable