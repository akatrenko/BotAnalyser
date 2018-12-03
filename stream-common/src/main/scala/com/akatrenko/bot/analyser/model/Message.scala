package com.akatrenko.bot.analyser.model

import com.akatrenko.bot.analyser.constant.MessageType

case class Message (unix_time: String, category_id: String, ip: String, types: String) extends Serializable {
  def checkType: Boolean = {
    types == MessageType.ClickType || types == MessageType.ViewType
  }
}
