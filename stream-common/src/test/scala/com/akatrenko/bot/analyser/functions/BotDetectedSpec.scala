package com.akatrenko.bot.analyser.functions

import java.time.Instant

import com.akatrenko.bot.analyser.constant.{MessageType, Rules}
import com.akatrenko.bot.analyser.model.Message
import org.scalatest.{FlatSpec, Matchers}

class BotDetectedSpec extends FlatSpec with Matchers with BotDetectedFunctions {
  "Dstream finder bot" should "return 1 bot from category count rule" in {
    val checkIp = "172.10.3.53"
    val inputPartnersData: Vector[Message] = Vector(
      Message(1543408780L, 1007, checkIp, "click"),
      Message(1543409783L, 1008, checkIp, "view"),
      Message(1543408786L, 1006, checkIp, "click"),
      Message(1543408790L, 1005, checkIp, "click"),
      Message(1543409789L, 1004, checkIp, "view"),
      Message(1543409793L, 1001, checkIp, "view"),
      Message(1543408780L, 1004, checkIp, "click"),
      Message(1543408780L, 1003, checkIp, "click"),
      Message(1543408780L, 1002, checkIp, "click")
    )

    val resultBot = inputPartnersData.groupBy(_.ip).flatMap(findBot("DStreamTest"))
    resultBot should have size 1
    resultBot.foreach { bot =>
      //println(bot)
      bot.ip should be(checkIp)
      bot.rule should be(categoryCount)
    }
  }

  "Dstream finder bot" should "return 1 bot from event rate rule" in {
    val checkIp = "172.10.3.53"
    val inputPartnersData: Vector[Message] = Vector(
      GeneratorSpec(checkIp, Rules.MaxEventRate + 5, 2),
      GeneratorSpec("172.10.3.54", 4, 2),
      GeneratorSpec("172.10.3.55", 4, 2)).flatMap(generateData)

    val resultBot = inputPartnersData.groupBy(_.ip).flatMap(findBot("DStreamTest"))
    resultBot should have size 1
    resultBot.foreach { bot =>
      //println(bot)
      bot.ip should be(checkIp)
      bot.rule should be(eventRateRule)
    }
  }

  "Dstream finder bot" should "return 2 bots from type different rule" in {
    val checkFirstIp = "172.10.3.53"
    val checkSecondIp = "172.10.3.54"
    val inputPartnersData: Vector[Message] = Vector(
      GeneratorSpec(checkFirstIp, 1, 2),
      GeneratorSpec(checkSecondIp, 99, 10),
      GeneratorSpec("172.10.3.55", 4, 2)).flatMap(generateData)

    val resultBot = inputPartnersData.groupBy(_.ip).flatMap(findBot("DStreamTest"))
    resultBot should have size 2
    resultBot.find(_.ip == checkFirstIp).map(_.rule) should be(Some(typeDiffRule))
    resultBot.find(_.ip == checkSecondIp).map(_.rule) should be(Some(typeDiffRule))
  }

  case class GeneratorSpec(ipStr: String, eventCount: Int, diffEventStepSec: Int)

  /**
    * Prepare input partners data
    * if eventCount in gen will be odd number then you will get a bot by type different rule
    *
    * @param gen parameters for generation
    * @return partners data
    */
  private def generateData(gen: GeneratorSpec): Vector[Message] = {
    val startTime = Instant.now().toEpochMilli
    val diffEventStep = gen.diffEventStepSec * 1000
    val eventTypeStep = 2 * 1000

    if (gen.eventCount == 1) {
      generateMsgEvent(gen.ipStr, MessageType.ClickType, gen.eventCount, eventTypeStep, startTime.+(diffEventStep), Vector.empty)
    } else if (gen.eventCount.%(2) == 0) {
      val clickEvent =
        generateMsgEvent(gen.ipStr, MessageType.ClickType, gen.eventCount./(2), eventTypeStep, startTime, Vector.empty)

      val viewEvent =
        generateMsgEvent(gen.ipStr, MessageType.ViewType, gen.eventCount./(2), eventTypeStep, startTime.+(diffEventStep), Vector.empty)
      clickEvent ++ viewEvent
    } else {
      val viewCount = gen.eventCount / (Rules.MinEventDifference + 2)
      val clickEvent =
        generateMsgEvent(gen.ipStr, MessageType.ClickType, gen.eventCount - viewCount, eventTypeStep, startTime, Vector.empty)

      val viewEvent =
        generateMsgEvent(gen.ipStr, MessageType.ViewType, viewCount, eventTypeStep, startTime.+(diffEventStep), Vector.empty)
      clickEvent ++ viewEvent
    }
  }

  private def generateMsgEvent(ipStr: String,
                               eventType: String,
                               eventCount: Int,
                               timeStep: Long,
                               unixTime: Long,
                               res: Vector[Message]): Vector[Message] = {
    if (eventCount != 0) {
      val msgTime = unixTime + timeStep
      val msg = Message(msgTime, 1, ipStr, eventType)
      generateMsgEvent(ipStr, eventType, eventCount.-(1), timeStep, msgTime, res :+ msg)
    } else {
      res
    }
  }
}
