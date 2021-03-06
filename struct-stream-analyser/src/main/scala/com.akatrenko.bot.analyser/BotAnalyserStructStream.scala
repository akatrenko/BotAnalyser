package com.akatrenko.bot.analyser

import com.akatrenko.bot.analyser.functions.{StreamFunctions, StructStreamFunctions}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object BotAnalyserStructStream extends StructStreamFunctions with StreamFunctions with Serializable {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(BotAnalyserStructStream.getClass.getName)
    val streamName = "BotAnalyserStructStream"

    val streamProperties = loadProperties(streamName)
    val checkpointDir = streamProperties.getProperty("sstreaming.checkpoint.dir")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .getOrCreate()

    readStructStream(spark, streamProperties, logger)
    spark.streams.awaitAnyTermination()
  }
}
