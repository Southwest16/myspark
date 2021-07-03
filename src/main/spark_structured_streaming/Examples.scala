package spark.streaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object Examples {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Examples")
            .getOrCreate()

        val streamingDF = spark.readStream.load()
        streamingDF.dropDuplicates("guid")
        streamingDF.withWatermark("eventTime", "guid")
            .dropDuplicates("guid", "eventTime")

        /*val impressions = spark.readStream.load()
        val clicks = spark.readStream.load()

        // 在event-time字段上应用watermarks
        val impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
        val clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

        // inner join
        impressionsWithWatermark.join(
            clicksWithWatermark,
            expr("""
                clickAdId = impressionAdId AND
                clickTime >= impressionTime AND
                clickTime <= impressionTime + interval 1 hour
                """)
        )

        // outer join
        impressionsWithWatermark.join(
            clicksWithWatermark,
            expr("""
                clickAdId = impressionAdId AND
                clickTime >= impressionTime AND
                clickTime <= impressionTime + interval 1 hour
                """),
            "left_outer"
        )*/

        /*val userSchema = new StructType().add("name", "string").add("age", "integer")
        val csvDF = spark
            .readStream
            .option("sep", ";")
            .schema(userSchema)
            .csv("/path/to/directory")*/


        /*import spark.implicits._
        val df: DataFrame = spark.readStream.load()
        val ds: Dataset[DeviceData] = df.as[DeviceData]

        df.select("device").where("signal > 10")    // using untyped APIs
        ds.filter(_.signal > 10).map(_.device)  //using typed APIs

        df.groupBy("deviceType").count()    // using untyped APIs
        import org.apache.spark.sql.expressions.scalalang.typed
        ds.groupByKey(_.deviceType).agg(typed.avg(_.signal))  //using typed APIs

        df.createOrReplaceTempView("updates")
        spark.sql("select count(*) from updates")

        df.isStreaming  //判断一个DataFrame/Dataset是否有流数据*/

    }

    // lot
    case class DeviceData(device: String, deviceType: String, signal: Double, time: DateTime)
}
