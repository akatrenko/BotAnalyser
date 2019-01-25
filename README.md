# BotAnalyser
Education project to detect bots in a matter of minutes, or even seconds.
The project should handle up to 200 000 events per minute and collect user click rate and transitions for last 10 minutes.

Requirements:
1. Operate at internet scale, so system should be able to scale to 100k-1M of inbound messages
2. Have SLA of 60 seconds latency (once we have all the required data from partners to make a decision, bot should be registered in cassandra within this time)
3. Should not block users forever, so there should be a configurable retention period (TTL), after which bot ip address is back to list of good guys
4. There is no guarantee that data about some ip will come in the same order as event happened, what's more some partners may delay events for tens of seconds

Data flow is simple and linear:

[partners] => [file disk] => [flume] => [kafka] => [spark] => [cassandra]

Implement following rules to detect bots:

1. Enormous event rate, e.g. more than 1000 request in 10 minutes*.
2. High difference between click and view events, e.g. (clicks/views) more than 3-5. Correctly process cases when there is no views.
3. Looking for many categories during the period, e.g. more than 5 categories in 10 minutes.

Implement following requirements using Streaming API v1 (DStream) and Streaming API v2(Structured streaming, Dataframes). And then compare the results of both computations.
