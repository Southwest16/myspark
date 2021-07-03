package demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
            .builder()
            .appName("JavaWordCount")
            .master("local[4]")
            .getOrCreate();

        JavaRDD<String> lines = spark.sparkContext()
            .textFile("F:\\Files\\file.txt", 2) //要给一个分区数
            .toJavaRDD();

        // 以空格分隔每行单词
        JavaRDD<String> words = lines
            .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        // 将每个单词转换成元组Tuple(其实就是key-value对)
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        // 以key聚合求值
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        // 打印结果
        counts.foreach(f -> System.out.println(f._1 +": "+ f._2));

        spark.stop();
    }
}
