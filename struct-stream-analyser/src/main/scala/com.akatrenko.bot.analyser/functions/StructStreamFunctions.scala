package com.akatrenko.bot.analyser.functions

import java.sql.Timestamp
import java.time.Instant
import java.util.Properties

import com.akatrenko.bot.analyser.model.{BadBot, Message, MessageAgg, MessageEvent}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.Logger
import com.akatrenko.bot.analyser.constant.MessageType._

import scala.util.{Failure, Success, Try}

trait StructStreamFunctions extends BotDetectedFunctions {
  private def deserializeStructStream(stream: Dataset[String], spark: SparkSession)
                                     (implicit mf: Manifest[MessageEvent]): Dataset[MessageEvent] = {
    import spark.implicits._
    stream.flatMap { row =>
      implicit val formats: DefaultFormats = DefaultFormats
      val parseResult = Try {
        parse(row).extract[Message]
      }
      parseResult match {
        case Success(msg: Message) =>
          msg.actionType match {
            case ViewType => Some(MessageEvent(new Timestamp(msg.unixTime), msg.categoryId, msg.ip, 0, 1))
            case ClickType => Some(MessageEvent(new Timestamp(msg.unixTime), msg.categoryId, msg.ip, 1, 0))
            case _ => None
          }
        case Failure(ex) =>
          ex.printStackTrace()
          None
      }
    }
  }

  def readStructStream(spark: SparkSession, streamProperties: Properties, logger: Logger): Unit = {
    import spark.implicits._

    val sourceName = "StructedStream"
    val triggerProcessTime = s"${streamProperties.getProperty("sstreaming.trigger.sec")} seconds"
    val windowDuration = s"${streamProperties.getProperty("sstreaming.window.duration.min")} minutes"
    val slideDuration = s"${streamProperties.getProperty("sstreaming.slide.duration.sec")} seconds"

    val topicName = streamProperties.getProperty("sstreaming.kafka.topic.name")
    val kafkaConsumerGroupId = streamProperties.getProperty("sstreaming.kafka.consumer.group")
    val bootstrapServers = streamProperties.getProperty("sstreaming.kafka.bootstrap.server")

    logger.info(
      s"""Kafka config: bootstrapServers = $bootstrapServers, kafkaConsumerGroupId = $kafkaConsumerGroupId,
         |topicName = $topicName""".stripMargin)

    val structStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .option("fetchOffset.numRetries", "3")
      .option("auto.offset.reset", "latest")
      .option("group.id", kafkaConsumerGroupId)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val messageStream: Dataset[MessageEvent] = deserializeStructStream(structStream, spark)

    val badBotStream = messageStream
      .withWatermark("unixTime", "20 minutes")
      .groupBy($"ip", window($"unixTime", windowDuration, slideDuration))
      .agg(sum($"clickEvent").alias("clicks"),
        sum($"viewEvent").alias("views"),
        collect_set($"categoryId").as("categories")
      )
      .drop("window")
      .as[MessageAgg]
      .filter(findBot _)
      .map(m => BadBot(m.ip, Timestamp.from(Instant.now()), sourceName))

    writeToCassandra(spark, badBotStream, streamProperties)
      .trigger(Trigger.ProcessingTime(triggerProcessTime))
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()
  }

  private def writeToCassandra(spark: SparkSession,
                               stream: Dataset[BadBot],
                               props: Properties): DataStreamWriter[BadBot] = {
    stream.writeStream
      .foreach(new CassandraWriter(CassandraConnector(spark.sparkContext.getConf), props))
  }
}
