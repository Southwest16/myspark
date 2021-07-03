package spark.streaming.structuredstreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWindowed {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
                " <window duration in seconds> [<slide duration in seconds>]")
            System.exit(1)
        }

        val host = args(0)
        val port = args(1)
        //
        val windowSize = args(2).toInt
        val slideSize = if (args.length == 3) windowSize else args(3).toInt
        if (slideSize > windowSize) {
            System.err.println("<slide duration> must be less than or equal to <window duration>")
        }

        // 指定window的宽度, 比如:10分钟, 1秒钟
        val windowDuration = s"$windowSize seconds"
        // 指定window的滑动间隔. 每个间隔都会产生一个新的window. 滑动间隔必须小于等于window宽度.
        val slideDuration = s"$slideSize seconds"

        val spark = SparkSession
            .builder()
            .appName("StructuredNetworkWordCountWindowed")
            .getOrCreate()

        import spark.implicits._

        val lines = spark.readStream
            .format("socket")
            .option("host", host)
            .option("port", port)
            .option("includeTimestamp", true)
            .load()

        val words = lines.as[(String, Timestamp)]
            .flatMap(line => line._1.split(" ").map(word => (word, line._2)))
            .toDF("word", "timestamp")

        val windowedCounts = words
            .groupBy(window($"timestamp", windowDuration, slideDuration), $"word")
            .count()
            .orderBy("window")

        val query = windowedCounts
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .start()

        query.awaitTermination()
    }
}
