package com.akatrenko.bot.analyser

import com.akatrenko.bot.analyser.functions.{DstreamFunctions, StreamFunctions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

object BotAnalyserDStream extends DstreamFunctions with StreamFunctions with Serializable {

  def main(args: Array[String]): Unit = {
    val streamName = "BotAnalyser"
    val logger = LoggerFactory.getLogger(BotAnalyserDStream.getClass.getName)
    val streamProperties = loadProperties(streamName)
    val checkpointDir = streamProperties.getProperty("dstreaming.checkpoint.dir")

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(streamName)
      .config("spark.sql.shuffle.partitions", "3")
      .config("spark.driver.memory", "2g")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.stats.autogather", "false")
      .getOrCreate()

    val streamingContext =
      StreamingContext.getOrCreate(checkpointDir, () => createStreamingContext(spark, streamProperties, logger))

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
