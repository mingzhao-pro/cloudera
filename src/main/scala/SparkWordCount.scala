import org.apache.spark._

/**
  * Created by saga on 6/6/17.
  */

object SparkWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("wordcount")
    val sc = new SparkContext(conf)
    /* local = master URL; Word Count = application name; */
    /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */
    /* Map = variables to work nodes */
    /*creating an inputRDD to read text file (in.txt) through Spark context*/
    val input = sc.textFile("hdfs://hadoop-vm:8020/user/saga/in.txt")
    /* Transform the inputRDD into countRDD */

    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    /* saveAsTextFile method is an action that effects on the RDD */
    count.saveAsTextFile("hdfs://hadoop-vm:8020/user/saga/out")
  }
}
