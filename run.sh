/Volumes/timdata/servers/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
     --class "com.airisdata.referenceapp.Twitter" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/twitter.jar \
     ${YOUR_OUTPUT_DIR:-/tmp/tweets} \
     ${NUM_TWEETS_TO_COLLECT:-25000} \
     ${OUTPUT_FILE_INTERVAL_IN_SECS:-10} \
     ${OUTPUT_FILE_PARTITIONS_EACH_INTERVAL:-1} \
     --consumerKey DvoZK6bObMIwZmnYlK5GP87rQ \
     --consumerSecret OOEoVl7EQngBGV3LSzf7PeEYe1i5SIW4Nr8RyckMXyBBHWuAHM \
     --accessToken 4796901285-xuvHrdY46hn3XbjmRkpCm7q4gaHI6v8TROJTBK8  \
     --accessTokenSecret Hp9dmwkcuGGyMQvwYcdjFStekbuVx0OwblHshZ0HraG88
