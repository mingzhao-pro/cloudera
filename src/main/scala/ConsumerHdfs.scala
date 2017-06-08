import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by saga on 6/7/17.
  */
object ConsumerHdfs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("twitter-consumer")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint")

  val kafkaConf = Map(
    "metadata.broker.list" -> "hadoop-vm:9092",
    "zookeeper.connect" -> "hadoop-vm:2181",
    "group.id" -> "kafka-spark-streaming-example",
    "zookeeper.connection.timeout.ms" -> "1000")

    val topicMap = Map("twitter" -> 1)
    val lines = KafkaUtils.createStream[Array[Byte], String,
      DefaultDecoder, StringDecoder](ssc, kafkaConf, topicMap,  StorageLevel.MEMORY_ONLY_SER).map(_._2)

    lines.foreachRDD(rdd => {
      rdd.saveAsTextFile("out")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
