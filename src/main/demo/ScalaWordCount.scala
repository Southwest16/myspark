package demo

import org.apache.spark.sql.SparkSession

object ScalaWordCount {
    def main(args: Array[String]): Unit = {
        // SparkSession实例, 也就是Spark任务的入口
        val spark = SparkSession
            .builder()
            .appName("ScalaWordCount")
            .master("local[4]") // 本地模式, 使用4个cpu cores
            .getOrCreate()

        //读取本地的一个txt文件, 里面是随意copy进去的一些英文文章片段
        val lines = spark.sparkContext.textFile("F:\\Files\\file.txt")

        // 文章中的单词都是以空格分隔的,
        // flatMap函数的作用就是对文件中的每一行单词以空格来分隔,
        // 得到是摊平的每个独立的单词, 如果打印出来的话就是每行一个单词。
        val words = lines.flatMap(_.split(" "))

        // 将每个单词转换成key-value对, value都是1,
        // 例如：spark 转换成 (spark, 1)。
        val ones = words.map(m => (m, 1))

        // 根据key进行聚合, 对于相同的key, 将其对应的value相加。
        val counts = ones.reduceByKey(_ + _)

        // 打印结果, 可以看到文件中每个单词的个数
        counts.foreach(f => {
            println(f._1 +": "+ f._2)
        })

        // 或者也可以保存到一个文件中。
        //counts.coalesce(1).saveAsTextFile("F:\\Files\\WordCountResult.txt")

        // 关闭SparkSession
        spark.close()
    }
}
