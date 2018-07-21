package xin.mrbear.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_Online {
    def main(args: Array[String]): Unit = {
        //设置 spark 的配置文件信息
        val sparkConf = new SparkConf().setAppName("WordCount")

        //构建sparkContext 对象 是程序入口
        val sc:SparkContext = new SparkContext(sparkConf)

        //读取文件
        val file:RDD[String] = sc.textFile(args(0))

        //对文件中的每一行单词进行 压平切分
        val words:RDD[String] = file.flatMap(_.split(" "))

        //对每一个单词计数为1，转换为（单词，1）
        val wordAndOne:RDD[(String,Int)] = words.map((_,1))

        //相同的单词进行汇总,前一个下滑线表示累加数据，后一个下划线表示新数据
        val result:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)

        //保存到HDFS
        result.saveAsTextFile(args(1))
        sc.stop()
    }
}
