package com.akatrenko.bot.analyser.model

case class StreamError[Message](entity: Message, exception: Throwable)
