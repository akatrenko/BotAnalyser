package com.akatrenko.bot.analyser.functions

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.Instant
import java.util.Properties

import com.akatrenko.bot.analyser.constant.MessageType.{ClickType, ViewType}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.Logger
import com.akatrenko.bot.analyser.model._
import com.datastax.spark.connector.writer.{TTLOption, WriteConf}
import com.datastax.spark.connector.streaming._

import scala.util.{Failure, Success, Try}

trait DstreamFunctions extends BotDetectedFunctions {

  def createStreamingContext(spark: SparkSession, streamProperties: Properties, logger: Logger): StreamingContext = {
    val sourceName = "DStream"
    val checkpointDir = streamProperties.getProperty("dstreaming.checkpoint.dir")

    val streamingIntervalSec = streamProperties.getProperty("dstreaming.interval.sec").toInt
    val windowDurationMin = streamProperties.getProperty("dstreaming.window.duration.min").toInt
    val windowSlideMin = streamProperties.getProperty("dstreaming.window.slide.min").toInt
    val durationCount = windowDurationMin / windowSlideMin

    val topicName = streamProperties.getProperty("dstreaming.kafka.topic.name")
    val kafkaConsumerGroupId = streamProperties.getProperty("dstreaming.kafka.consumer.group")
    val bootstrapServers = streamProperties.getProperty("dstreaming.kafka.bootstrap.server")

    val cassandraKeySpaceName = streamProperties.getProperty("dstreaming.cassandra.keyspace.name")
    val cassandraTableName = streamProperties.getProperty("dstreaming.cassandra.table.name")
    val cassandraTTL = streamProperties.getProperty("dstreaming.cassandra.ttl").toInt

    logger.info(
      s"""Kafka config: bootstrapServers = $bootstrapServers, kafkaConsumerGroupId = $kafkaConsumerGroupId,
         |topicName = $topicName; Cassandra config: cassandraKeySpaceName = $cassandraKeySpaceName,
         |cassandraTableName = $cassandraTableName, cassandraTTL = $cassandraTTL;
         |Stream config: windowDuration in minute = $windowDurationMin,
         |streamingInterval in second = $streamingIntervalSec
       """.stripMargin)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(streamingIntervalSec))
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map(
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaConsumerGroupId,
      "auto.offset.reset" -> streamProperties.getProperty("auto.offset.reset", "latest"),
      "enable.auto.commit" -> streamProperties.getProperty("enable.auto.commit", "false")
    )

    val rowStream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      Subscribe[String, String](Set(topicName), kafkaParams))

    val messageStream = transformDStream(rowStream.map(_.value()))
      .filter(eitherMsg => eitherMsg.isLeft && eitherMsg.left.get.checkType)
      .flatMap { leftMsg =>
        val messageAgg = leftMsg.left.get
        val startPeriod = messageAgg.eventTime.map { minDates =>
          val minDate = minDates.toLocalDateTime.minusMinutes(durationCount * Minutes(windowSlideMin).milliseconds)
          if (minDate.getMinute % windowSlideMin == 0) {
            Timestamp.valueOf(minDate)
          } else {
            val truncateMinDate = minDate.truncatedTo(ChronoUnit.HOURS)
            val minutes = minDate.getMinute./(windowSlideMin) * windowSlideMin
            Timestamp.valueOf(truncateMinDate.plusMinutes(minutes))
          }
        }

        (0 to durationCount).map { i =>
          val eventTime = startPeriod.map(startDate =>
            Timestamp.from(startDate.toInstant.plusMillis(i * Minutes(windowSlideMin).milliseconds)))
          messageAgg.copy(eventTime = eventTime)
        }
      }
      .map { msg =>
        val startTime = msg.eventTime.get
        val endTime = new Timestamp(msg.eventTime.get.getTime + Minutes(windowDurationMin).milliseconds)
        ((msg.ip, (startTime, endTime)), msg.copy(wind = Some(startTime, endTime)))
      }

    val badBotStream = messageStream
      .reduceByKey((msg1: MessageAgg, msg2: MessageAgg) => msg1.+(msg2)(windowDurationMin))
      .mapWithState(
        StateSpec
          .function(stateFunction _)
          .timeout(Minutes(windowDurationMin))
      )
      .filter(findBot)
      .map(msgAgg => BadBot(msgAgg.ip, Timestamp.from(Instant.now()), sourceName))

    badBotStream.saveToCassandra(keyspaceName = cassandraKeySpaceName,
      tableName = cassandraTableName,
      writeConf = WriteConf(ttl = TTLOption.constant(cassandraTTL))
    )
    ssc
  }

  private def stateFunction(key: (String, (Timestamp, Timestamp)),
                            value: Option[MessageAgg],
                            state: State[MessageAgg]): MessageAgg = {
    value.foreach { msg =>
      if (!state.isTimingOut) {
        state.update(
          if (state.exists()) {
            state.get().+(msg)(10)
          } else {
            msg
          }
        )
      }
    }
    state.get()
  }

  private def transformDStream(stream: DStream[String])
                              (implicit mf: Manifest[MessageAgg]): DStream[Either[MessageAgg, StreamError[String]]] = {
    stream.transform(rdd => {
      rdd.mapPartitions(jsonIterator => {
        implicit val formats: DefaultFormats = DefaultFormats
        jsonIterator.map { json =>
          //println(json)
          val parseResult = Try {
            parse(json).extract[Message]
          }
          parseResult match {
            case Success(msg: Message) =>
              val eventTime = Instant.ofEpochSecond(msg.unixTime).truncatedTo(ChronoUnit.MINUTES)
              Left(msg.actionType match {
                case ViewType => MessageAgg(Set(msg.categoryId), msg.ip, 0, 1, Some(Timestamp.from(eventTime)))
                case ClickType => MessageAgg(Set(msg.categoryId), msg.ip, 1, 0, Some(Timestamp.from(eventTime)))
                case _ => MessageAgg(Set(msg.categoryId), msg.ip, 0, 0, Some(Timestamp.from(eventTime)))
              })
            case Failure(ex) =>
              ex.printStackTrace()
              Right(StreamError(json, ex))
          }
        }
      })
    })
  }
}
