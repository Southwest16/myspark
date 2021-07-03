package spark.streaming.structuredstreaming

import org.apache.spark.sql.SparkSession

/**
  * To run this on your local machine, you need to first run a Netcat server
  * `$ nc -lk 9999`
  * and then run the example
  * `/app/spark/bin/spark-submit ...
  * localhost 9999`
  */
object StructuredNetworkWordCount {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println("Usage: StructuredNetworkWordCount <hostname> <port>")
            System.exit(1)
        }

        val host = args(0)
        val port = args(1).toInt

        val spark = SparkSession
            .builder()
            .appName("StructuredNetworkWordcCount")
            .getOrCreate()

        import spark.implicits._

        // Create DataFrame representing the stream of input lines from connection to host:port
        val lines = spark.readStream
            .format("socket")
            .option("host", host)
            .option("port", port)
            .load()

        // Split the lines into words
        val words = lines.as[String].flatMap(_.split(" "))

        // Genderate running word count
        val wordCounts = words.groupBy("value").count()

        // Start running the query that prints the running counts to console
        val query = wordCounts.writeStream
            .outputMode("update")
            .format("console")
            .start()

        query.awaitTermination()

    }
}
/*
/app/spark/bin/spark-submit \
--class com.lqwork.structuredstreaming.StructuredNetworkWordCount \
--master yarn \
--deploy-mode client \
--driver-memory 2G \
--executor-memory 1G \
--executor-cores 1 \
--num-executors 5 \
/home/bigdatadev/taf/jars/lqwork.jar \
localhost \
9999
*/