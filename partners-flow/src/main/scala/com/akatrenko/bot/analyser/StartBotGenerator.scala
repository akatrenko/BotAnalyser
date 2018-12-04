package com.akatrenko.bot.analyser

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.sys.process._

object StartBotGenerator {
  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load
    val logger = LoggerFactory.getLogger(StartBotGenerator.getClass.getName)

    val bootstrapServer = config.getString("kafka.bootstrapServer")
    val partnerSourceTopicName = config.getString("kafka.partnerSourceTopicName")

    val directoryPath = config.getString("app.rootPath")
    val generatorScriptName = config.getString("app.botgenScript")
    val dataFileName = config.getString("app.partnerSourceFile")
    val botCount = config.getInt("app.botCount")

    /*val netcatHost = config.getString("netcat.host")
    val netcatPort = config.getString("netcat.port")
    val netcatPath = s"$netcatHost $netcatPort"*/

    logger.info(s"""Loaded config: bootstrap server = $bootstrapServer,
                "partners source topic in Kafka = $partnerSourceTopicName,
                directory path = $directoryPath,
                generator script name = $generatorScriptName, source file name = $dataFileName""")

    //todo start generator mode in interval?
    s"python3 $directoryPath/generator/$generatorScriptName -f $directoryPath/generator/$dataFileName -b $botCount" !
      ProcessLogger(stdout append _, stderr append _)
  }
}
