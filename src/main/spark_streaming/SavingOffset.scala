package sparkstreaming

import kafka.utils.ZkUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Assign
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}

/**
  * https://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming/
  */
class SavingOffset {
    def createDirectStreamFromKafka (): Unit = {
        val conf = new SparkConf().setAppName("")
        val ssc = new StreamingContext(conf, Milliseconds(1))

        // Kafka参数配置
        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "bigdata07:9097,bigdata08:9098,bigdata09:9099",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "",
            "autp.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topic = "app_logs"
        val zkQuorum = ""

        val fromOffsets = getLastCommitedOffsets(topic, "", "", "", "", 0, 0)
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
        )

        stream.foreachRDD{ rdd =>
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        }

    }

    // 从Hbase读取offset
    def getLastCommitedOffsets(TOPIC_NAME: String,
                               GROUP_ID: String,
                               hbaseTableName: String,
                               zkQuorum: String,
                               zkRootDir: String,
                               sessionTimeout: Int,
                               connectionTimeout: Int): Map[TopicPartition, Long] = {

        val zkUrl = zkQuorum + "/" + zkRootDir
        val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
        val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
        val zkNumberOfPartitionForTopic = zkUtils.getPartitionsForTopics(Seq(TOPIC_NAME)).get(TOPIC_NAME).toList.head.size
        zkClientAndConnection._1.close()
        zkClientAndConnection._2.close()

        val hbaseInfo = getHbaseInfo(TOPIC_NAME, GROUP_ID)
        val startRow = TOPIC_NAME + ":" + GROUP_ID + ":" + String.valueOf(System.currentTimeMillis())
        val stopRow = TOPIC_NAME + ":" + GROUP_ID + ":" + 0
        val scan = new Scan()
        val scanner = hbaseInfo.table.getScanner(scan
            .setStartRow(startRow.getBytes())
            .setStopRow(stopRow.getBytes())
            .setReversed(true))
        val result = scanner.next()

        var hbaseNumberOfPartitionForTopic = 0
        if (result != null) {
            hbaseNumberOfPartitionForTopic = result.listCells().size()
        }

        val fromOffsets = collection.mutable.Map[TopicPartition, Long]()
        // 流任务第一次启动时，Hbase中没有记录任何有关这个topic所有分区的偏移量，那么就从0开始消费
        if (hbaseNumberOfPartitionForTopic == 0) {
            for (partition <- 0 to zkNumberOfPartitionForTopic - 1) {
                fromOffsets += new TopicPartition(TOPIC_NAME, partition) -> 0
            }
        // 对于正在运行的流任务，增加了Kafka的分区数，新增的分区是不能被正在运行的任务感知到的，
        // 如果要让任务感知到新增的分区，程序就必须重启，同时，也要把新增分区的偏移量保存起来。
        } else if (zkNumberOfPartitionForTopic > hbaseNumberOfPartitionForTopic) {
            for (partition <- 0 to hbaseNumberOfPartitionForTopic - 1) {
                val fromOffset = Bytes.toString(result.getValue(
                    Bytes.toBytes("offsets"),
                    Bytes.toBytes(partition.toString)))
                fromOffsets += new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong
            }

            for (partition <- hbaseNumberOfPartitionForTopic to zkNumberOfPartitionForTopic - 1) {
                fromOffsets += new TopicPartition(TOPIC_NAME, partition) -> 0
            }
        } else {// 流任务停止后重新启动
            for (partition <- 0 to hbaseNumberOfPartitionForTopic - 1) {
                val fromOffset = Bytes.toString(result.getValue(
                    Bytes.toBytes("offsets"),
                    Bytes.toBytes(partition.toString)))
                fromOffsets += new TopicPartition(TOPIC_NAME, partition) -> fromOffset.toLong
            }
        }

        scanner.close()
        hbaseInfo.connection.close()
        fromOffsets.toMap
    }


    // 保存offset到Hbase
    def saveOffsets(TOPIC_NAME: String,
                    GROUP_ID: String,
                    offsetRanges: Array[OffsetRange],
                    hbaseTableName: String,
                    batchTime: Time): Unit = {
        val hbaseInfo = getHbaseInfo(TOPIC_NAME, GROUP_ID)

        // rowKey
        val rowKey = TOPIC_NAME+ ":" + GROUP_ID + ":" + String.valueOf(batchTime.milliseconds)
        // Used to perform Put operations for a single row.
        val put = new Put(rowKey.getBytes())
        for (offset <- offsetRanges) {
            put.addColumn(Bytes.toBytes("offsets"), Bytes.toBytes(offset.partition.toString), Bytes.toBytes(offset.untilOffset.toString))
        }

        hbaseInfo.table.put(put)
        hbaseInfo.connection.close()
    }

    // Hbase连接、表
    def getHbaseInfo(TOPIC_NAME: String, GROUP_ID: String): HbaseInfo = {
        val conf = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(conf)   //创建Hbase连接
        val table = connection.getTable(TableName.valueOf("hbaseTableName"))    //存放offset的表
        HbaseInfo.apply(connection, table)
    }

    case class HbaseInfo (connection: Connection, table: Table)

}
