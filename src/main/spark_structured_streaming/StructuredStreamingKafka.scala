package spark.streaming.structuredstreaming

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType}

object StructuredStreamingKafka {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("test")
            .getOrCreate()

        import spark.implicits._

        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "bigdata07:9097,bigdata08:9098,bigdata09:9099")
            .option("partition.assignment.strategy", "range")
            .option("subscribe", "app_logs")
            .load()

        // json结构
        val schema = new StructType()
            .add("create_time", StringType)
            .add("imei", StringType)
            .add("mac", StringType)
            .add("os_version", StringType)
            .add("bu", StringType)
            .add("app_version", StringType)
            .add("user_uuid", StringType)
            .add("net", StringType)
            .add("ip", StringType)
            .add("gps", StringType)
            .add("events", new StructType()
                .add("event_type", StringType)
                .add("event_id", StringType)
                .add("event_ts", StringType)
                .add("event_arg1", StringType)
                .add("event_arg2", StringType)
                .add("event_arg3", StringType)
            )

        // json串映射成表
        val row = df.select($"key".cast("string"), $"topic".cast("string"), $"offset".cast("string"),
            from_json($"value".cast("string"), schema).as("value"))
            .selectExpr("key", "topic", "offset",
                "value.create_time",
                "value.imei",
                "value.mac",
                "value.os_version",
                "value.bu",
                "value.app_version",
                "value.user_uuid",
                "value.net",
                "value.ip",
                "value.gps",
                "cast(value.events as string)")


        // 输出到MySQL
        val config = ConfigFactory.load()
        val driver = config.getString("application.driver")
        val url = config.getString("application.url")
        val userName = config.getString("application.username")
        val password = config.getString("application.password")


        // 输出到指定Hive表的HDFS目录
        import scala.concurrent.duration._
        row.writeStream
            .format("parquet")
            .option("checkpointLocation", "/third_party_data/checkpoint")
            .option("path", "/user/hive/warehouse/test.db/structuredstreaming_kafka")
            .trigger(Trigger.ProcessingTime(10.seconds))
            .trigger(Trigger.Continuous("1 second"))
            .outputMode("append") //Data source parquet does not support Update output mode
            .start()

        //windowed aggregations
        //每5分钟统计一下过去10分钟窗口的单词数
        row.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
          .count()

        //Handling Late Data and Watermarking
        row.withWatermark("timestamp", "10 minutes")
          .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word")
          .count()


        // 控制台打印
        val query = row
            .writeStream
            .outputMode("update")
            .format("console")
            .start()

        query.awaitTermination()
    }
}
