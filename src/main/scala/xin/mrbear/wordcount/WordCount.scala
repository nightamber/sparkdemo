package xin.mrbear.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        //1.创建sparkcontext对象
        val sparkConf:SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)

        sc.setLogLevel("WARM")

        val data:RDD[String] = sc.textFile("e:\\bb.txt")

        val words:RDD[String] = data.flatMap(_.split(" "))

        val wordAndOne:RDD[(String,Int)] = words.map((_,1))

        val result:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)

        val sortResult: RDD[(String, Int)] = result.sortBy(_._2,false)

        val finalResult: Array[(String, Int)] = sortResult.collect()

        finalResult.foreach(x=>println(x))

        sc.stop()

    }
}
