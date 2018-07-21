package xin.mrbear.wordcount;

import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCountJavaLamba {

    public static void main(String[] args) {
        //创建 sparkConf对象
        SparkConf conf = new SparkConf().setAppName("wordCountJava")
            .setMaster("local[2]");

        //根据sparkConf对象创建 javaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取文件
        String path = "e:\\aa.txt";
        JavaRDD<String> javaRDD = sc.textFile(path);

        //对读取的每一行数据进行切分
        JavaRDD<String> words = javaRDD
            .flatMap(lines -> Arrays.asList(lines.split(" ")).iterator());

        //对出现的每一个单词记为1，将原来的字符串转换元祖
        JavaPairRDD<String, Integer> wordAndOnes = words
            .mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        //将单词进行累加
        JavaPairRDD<String, Integer> wordcount = wordAndOnes
            .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> reverseWordCount = wordcount.mapToPair(word -> new Tuple2<Integer, String>(word._2, word._1));

        JavaPairRDD<String, Integer> finalResult = reverseWordCount.sortByKey(false).mapToPair(word -> new Tuple2<String, Integer>(word._2, word._1));

        System.out.println(finalResult.collect());

        sc.stop();


    }
}
