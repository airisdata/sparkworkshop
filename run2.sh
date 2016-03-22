/Volumes/data/servers/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
     --class "com.databricks.apps.twitter_classifier.ExamineAndTrain" \
     --master ${YOUR_SPARK_MASTER:-local[4]} \
     target/scala-2.10/spark-twitter-lang-classifier-assembly-1.0.jar \
     "${YOUR_TWEET_INPUT:-/tmp/tweets/tweets*/part-*}" \
     ${OUTPUT_MODEL_DIR:-/tmp/tweets/model} \
     ${NUM_CLUSTERS:-10} \
     ${NUM_ITERATIONS:-20}
