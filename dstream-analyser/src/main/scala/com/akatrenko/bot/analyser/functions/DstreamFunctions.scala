package com.akatrenko.bot.analyser.functions

import java.util.Properties

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
    val windowDurationSec = streamProperties.getProperty("dstreaming.window.duration.sec").toInt

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
         |Stream config: windowDuration in second = $windowDurationSec,
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

    val messageStream = deserializeDStream[Message](rowStream.map(_.value()))
      .filter(eitherMsg => eitherMsg.isLeft && eitherMsg.left.get.checkType)
      .map(_.left.get)

    val badBotStream = messageStream
      .map(msg => ((msg.ip, msg.unixTime.toLong), msg))
      .reduceByKeyAndWindow((_, msg) => msg, Seconds(windowDurationSec))
      .mapWithState(
        StateSpec
          .function(stateFunction _)
          .timeout(Seconds(windowDurationSec))
      )
      .reduceByKey((msgLeft, msgRight) => msgLeft ++ msgRight)
      .flatMap(findBot(sourceName))

    badBotStream.saveToCassandra(keyspaceName = cassandraKeySpaceName,
      tableName = cassandraTableName,
      writeConf = WriteConf(ttl = TTLOption.constant(cassandraTTL))
    )
    ssc
  }

  private def stateFunction(key: (String, Long),
                            value: Option[Message],
                            state: State[List[(Message, Long)]]): (String, List[Message]) = {
    value.foreach { messageValue =>
      if (!state.isTimingOut) {
        state.update(
          if (state.exists) {
            state.get.filter(stateValue => stateValue._2 < (key._2 - 600)) :+ (messageValue, key._2)
          }
          else {
            List((messageValue, key._2))
          }
        )
      }
    }
    (key._1, state.get.map(stateValue => stateValue._1))
  }

  private def deserializeDStream[Message](stream: DStream[String])
                                         (implicit mf: Manifest[Message]): DStream[Either[Message, StreamError[String]]] = {
    stream.transform(rdd => {
      rdd.mapPartitions(jsonIterator => {
        implicit val formats: DefaultFormats = DefaultFormats
        jsonIterator.map { json =>
          //println(json)
          val parseResult = Try {
            parse(json).extract[Message]
          }
          parseResult match {
            case Success(msg: Message) => Left(msg)
            case Failure(ex) =>
              ex.printStackTrace()
              Right(StreamError(json, ex))
          }
        }
      })
    })
  }
}
