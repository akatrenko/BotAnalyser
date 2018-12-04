package com.akatrenko.bot.analyser.model

import com.akatrenko.bot.analyser.constant.MessageType

case class Message (unixTime: String, categoryId: String, ip: String, actionType: String) extends Serializable {
  def checkType: Boolean = {
    actionType == MessageType.ClickType || actionType == MessageType.ViewType
  }
}
