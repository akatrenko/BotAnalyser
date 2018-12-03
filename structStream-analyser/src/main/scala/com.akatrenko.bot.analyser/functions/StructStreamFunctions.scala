package com.akatrenko.bot.analyser.functions

import java.util.Properties

import com.akatrenko.bot.analyser.model.{BadBot, Message}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.Logger

import scala.collection.immutable
import scala.util.{Failure, Success, Try}

trait StructStreamFunctions extends BotDetectedFunctions {
  private def deserializeDatasetStream[T <: Product](stream: Dataset[String], spark: SparkSession)
                                                    (implicit mf: Manifest[T]): Dataset[T] = {
    import spark.implicits._
    stream.flatMap { row =>
      implicit val formats: DefaultFormats = DefaultFormats
      val parseResult = Try {
        parse(row).extract[T]
      }
      parseResult match {
        case Success(msg: T) => Seq(msg)
        case Failure(ex) =>
          ex.printStackTrace()
          immutable.Seq[T]()
      }
    }
  }

  def readStructStream(spark: SparkSession, streamProperties: Properties, logger: Logger): Unit = {
    import spark.implicits._

    val sourceName = "StructedStream"

    val topicName = streamProperties.getProperty("dstreaming.kafka.topic.name")
    val kafkaConsumerGroupId = streamProperties.getProperty("dstreaming.kafka.consumer.group")
    val bootstrapServers = streamProperties.getProperty("dstreaming.kafka.bootstrap.server")

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

    val messageStream: Dataset[Message] = deserializeDatasetStream(structStream, spark)

    //who is a bot??
    implicit val messageEncoder: Encoder[Message] = org.apache.spark.sql.Encoders.kryo[Message]
    val badBotStream = messageStream.groupByKey(_.ip).flatMapGroups { case (k, v) => findBot(sourceName)((k, v.toIterable)) }

    writeToCassandra(spark, badBotStream, streamProperties)
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

  }

  private def writeToCassandra(spark: SparkSession, datastream: Dataset[BadBot], props: Properties): DataStreamWriter[BadBot] = {
    datastream.writeStream
      .foreach(new CassandraWriter(CassandraConnector(spark.sparkContext.getConf), props))
  }
}
