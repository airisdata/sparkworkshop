/Volumes/data/servers/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.Predict" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     ${YOUR_MODEL_DIR:-/tmp/tweets/model} \
     ${CLUSTER_TO_FILTER:-7} \
     --consumerKey DvoZK6bObMIwZmnYlK5GP87rQ \
     --consumerSecret OOEoVl7EQngBGV3LSzf7PeEYe1i5SIW4Nr8RyckMXyBBHWuAHM \
     --accessToken 4796901285-xuvHrdY46hn3XbjmRkpCm7q4gaHI6v8TROJTBK8  \
     --accessTokenSecret Hp9dmwkcuGGyMQvwYcdjFStekbuVx0OwblHshZ0HraG88
