# BotAnalyser
Education project to detect bots in a matter of minutes, or even seconds.
The project should handle up to 200 000 events per minute and collect user click rate and transitions for last 10 minutes.

Data flow is simple and linear:

[partners] => [file disk] => [flume] => [kafka] => [spark] => [cassandra]
