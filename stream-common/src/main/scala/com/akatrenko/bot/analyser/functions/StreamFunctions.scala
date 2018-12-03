package com.akatrenko.bot.analyser.functions

import java.io.FileInputStream
import java.util.Properties

trait StreamFunctions {
  def loadProperties(streamName: String): Properties = {
    val streamProperties = new Properties()
    sys.env.get("stream_properties")
      .map(str => {
        streamProperties.load(new FileInputStream(str))
        str
      })
      .getOrElse(
        {
          streamProperties.load(this.getClass.getClassLoader.getResourceAsStream(s"$streamName.properties"))
          s"$streamName.properties"
        })
    streamProperties
  }
}
