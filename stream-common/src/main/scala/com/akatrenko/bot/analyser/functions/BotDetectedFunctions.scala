package com.akatrenko.bot.analyser.functions

import com.akatrenko.bot.analyser.model.MessageAgg
import com.akatrenko.bot.analyser.constant.Rules._

/**
  * Rules to detect bot
  */
trait BotDetectedFunctions {

  def findBot(msg: MessageAgg): Boolean = {
    (msg.clicks + msg.views) > MaxEventRate ||
      (msg.clicks / math.max(msg.views, 1)) > MinEventDifference ||
      msg.categories.size > MaxCategories
  }
}
