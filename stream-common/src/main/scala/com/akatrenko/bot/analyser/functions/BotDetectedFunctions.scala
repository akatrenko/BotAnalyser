package com.akatrenko.bot.analyser.functions

import java.sql.Timestamp
import java.time.Instant

import com.akatrenko.bot.analyser.constant.{MessageType, Rules}
import com.akatrenko.bot.analyser.model.{BadBot, Message}

/**
  * Rules to detect bot
  */
trait BotDetectedFunctions {
  private[functions] val eventRateRule = "Event more than 1000 request"
  private[functions] val typeDiffRule = "High difference between click and view events"
  private[functions] val categoryCount = "Many categories during the period"

  def findBot(sourceName: String)
             (msgEvent: (String, Iterable[Message])): Option[BadBot] = {
    val currentTimestamp = Timestamp.from(Instant.now())
    if (checkEventRateRule(msgEvent)) {
      Some(BadBot(msgEvent._1, eventRateRule, currentTimestamp, sourceName))
    } else if (checkTypeDiffRule(msgEvent)) {
      Some(BadBot(msgEvent._1, typeDiffRule, currentTimestamp, sourceName))
    } else if (checkCategoryCount(msgEvent)) {
      Some(BadBot(msgEvent._1, categoryCount, currentTimestamp, sourceName))
    } else {
      None
    }
  }

  /**
    * Enormous event rate, e.g. more than 1000 request in periods
    */
  private[functions] def checkEventRateRule(ipGroup: (String, Iterable[Message])): Boolean = {
    ipGroup._2.size >= Rules.MaxEventRate
  }

  /**
    * High difference between click and view events, e.g. (clicks/views) more than 3-5
    * include process cases when there is no views
    */
  private[functions] def checkTypeDiffRule(ipGroup: (String, Iterable[Message])): Boolean = {
    val typeGroup = ipGroup._2.groupBy(_.actionType)
    val clickGroupOpt = typeGroup.get(MessageType.ClickType)
    val viewGroupOpt = typeGroup.get(MessageType.ViewType)

    viewGroupOpt.isEmpty || /*clickGroupOpt.isEmpty ||*/
      clickGroupOpt.exists { clickEvent =>
        viewGroupOpt.exists(viewEvent => clickEvent.size / viewEvent.size > Rules.MinEventDifference)
      }

  }

  /**
    * Many categories during the period, e.g. more than 5 categories in 10 periods
    */
  private[functions] def checkCategoryCount(ipGroup: (String, Iterable[Message])): Boolean = {
    ipGroup._2.filter(ipEvent => ipEvent.actionType == MessageType.ClickType).groupBy(_.categoryId).size > Rules.MaxCategories
  }
}
