package xin.mrbear.wordcount;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCountJava {

    public static void main(String[] args) {
        //构建sparkconf,设置配置信息
        SparkConf sparkConf = new SparkConf().setAppName("WordCount_JAVA")
            .setMaster("local[2]");
        //构建java版本的sparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //读取文件数据
        JavaRDD<String> dataRDD = jsc.textFile("e:\\aa.txt");

        //对一行单词进行切分
        JavaRDD<String> wordsRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                return Arrays.asList(words).iterator();
            }
        });

        //给每个单词记录为1
        // Spark 为包含键值对类型的 RDD 提供了一些专有的操作。这些 RDD 被称为PairRDD。
        // mapToPair 函数会对一个 RDD 中的每个元素调用 f 函数，其中原来 RDD 中的每一个元素都是 T 类型的，
        // 调用 f 函数后会进行一定的操作把每个元素都转换成一个<K2,V2>类型的对象,其中 Tuple2 为多元组
        JavaPairRDD<String, Integer> wordAndOnePairRDD = wordsRDD
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String word) throws Exception {
                    return new Tuple2<String, Integer>(word, 1);
                }
            });

        //相同单词出现的次数累加
        JavaPairRDD<String, Integer> resultJavaPairRDD = wordAndOnePairRDD
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });

        //反正顺序
        JavaPairRDD<Integer, String> reverseJavaPairRDD = resultJavaPairRDD
            .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2)
                    throws Exception {
                    return new Tuple2<Integer, String>(stringIntegerTuple2._2,
                        stringIntegerTuple2._1);


                }
            });

        //把每个单词出现的次数作为key 进行排序， 并且在通过mapToPair进行翻转顺序后输出
        JavaPairRDD<String, Integer> sortJavaPairRDD = reverseJavaPairRDD.sortByKey(false)
            .mapToPair(
                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2)
                        throws Exception {
                        return new Tuple2<String, Integer>(integerStringTuple2._2,integerStringTuple2._1);
                    }
                });
        System.out.println(sortJavaPairRDD.collect());

        jsc.stop();
    }
}
