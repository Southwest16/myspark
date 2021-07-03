package sparkstreaming

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Map

object SSKafkaHBase {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("test")
            .setMaster("local[5]")
        val ssc = new StreamingContext(conf, Seconds(2))
        val config = ConfigFactory.load()

        val brokers = config.getString("application.brokers") //broker列表
        val kafkaParams: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "0001",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = config.getString("application.topics").split(",").toSet //topic列表

        //HBase连接
        val hConf = HBaseConfiguration.create()
        val connection = ConnectionFactory.createConnection(hConf) //创建连接
        val admin = connection.getAdmin
        val tableName = TableName.valueOf("TEST.KAFKAOFFSET") //HBase表名

        val isExists = admin.tableExists(tableName) //判断存放offset的表是否存在
        var stream: InputDStream[ConsumerRecord[String, String]] = null

        if (isExists) {
            val hTable = connection.getTable(tableName) //获取表
            val rs = hTable.getScanner(Bytes.toBytes("cf_offset")).iterator()

            var fromOffsets: Map[TopicPartition, Long] = Map()
            var topic: String = null
            var partition = 0
            var offset = 0L
            while (rs.hasNext) {
                val next = rs.next()
                for (kv <- next.rawCells()) {
                    val rowKey = Bytes.toString(CellUtil.cloneRow(kv))
                    val array = rowKey.split("\\-")
                    topic = array(0)
                    partition = array(1).toInt
                    offset = Bytes.toLong(CellUtil.cloneValue(kv))
                }
                fromOffsets += (new TopicPartition(topic, partition) -> offset)
            }

            stream = KafkaUtils.createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Assign[String, String](
                    fromOffsets.keys,
                    kafkaParams,
                    fromOffsets)
            )
        }
        else { //如果不存在之前数据的offset, 创建一个保存offset的表
            stream = KafkaUtils.createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams)
            )

            val descriptor = new HTableDescriptor(TableName.valueOf("TEST.KAFKAOFFSET")) //HBase表名
            descriptor.addFamily(new HColumnDescriptor("cf_offset")) //列族名
            descriptor.addFamily(new HColumnDescriptor("cf_data")) //列族名
            admin.createTable(descriptor)
        }


        //offset保存, 以及结果输出
//        stream.foreachRDD(rdd => {
//            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//            // begin your transaction
//
//            // update results
//            // update offsets where the end of existing offsets matches the beginning of this batch of offsets
//            // assert that offsets were updated correctly
//
//            // end your transaction
//            val ht = connection.getTable(tableName)
//            var put: Put = null
//            for (offsetRange <- offsetRanges) {
//                put = new Put(Bytes.toBytes(offsetRange.topic + "-" + offsetRange.partition))
//                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
//                ht.put(put)
//            }
//            rdd.map(_.value()).foreach(println)
//        })
        stream.foreachRDD(rdd => {
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            val map = rdd.mapPartitionsWithIndex((pid, iter) => {
                val m = Map[Int, String]()
                while (iter.hasNext) {
                    m += (pid -> iter.next().toString)
                }
                m.iterator
            }).collectAsMap()

            val ht = connection.getTable(tableName)
            var put: Put = null
            for (offsetRange <- offsetRanges) {
                val partition = offsetRange.partition
                val v = map.get(partition).getOrElse("")

                put = new Put(Bytes.toBytes(offsetRange.topic + "-" + offsetRange.partition))
                put.addColumn(Bytes.toBytes("cf_offset"), Bytes.toBytes("offset"), Bytes.toBytes(offsetRange.untilOffset))
                put.addColumn(Bytes.toBytes("cf_data"), Bytes.toBytes("data"), Bytes.toBytes(v))
                ht.put(put)
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
