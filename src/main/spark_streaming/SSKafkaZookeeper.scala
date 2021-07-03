package sparkstreaming

import com.typesafe.config.{Config, ConfigFactory}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Map

object SSKafkaZookeeper {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("test")
            .setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(2))
        val config = ConfigFactory.load()

        save(ssc, config)

        ssc.start()
        ssc.awaitTermination()
    }

    def save(ssc: StreamingContext, config: Config): Unit = {
        val messages = createCustomDirectKafkaStream(ssc, config)
        messages.map(_.value()).foreachRDD(rdd => {
            if (rdd.isEmpty()) {
                println("++++++++++++++++++")
            } else {
                rdd.foreach(println)
            }
        })
    }

    def createCustomDirectKafkaStream(ssc: StreamingContext, config: Config): InputDStream[ConsumerRecord[String, String]] = {
        // Kafka参数
        val brokers = config.getString("application.brokers")
        val kafkaParams: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "0001",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        //主题
        val topics = config.getString("application.topics").split(",").toSet

        // Zookeeper参数
        val zkServers = config.getString("application.zkServers")
        val zkClient = new ZkClient(zkServers, 10000, 10000)
        val zkPath = config.getString("application.zkPath")
        val zkConnection = new ZkConnection(zkServers)
        val zkUtils = new ZkUtils(zkClient, zkConnection, false)

        //Direct方式连接Kafka
        var stream: InputDStream[ConsumerRecord[String, String]] = null
        if (zkUtils.pathExists(zkPath)) {
            //读取offset
            val fromOffsets = readOffsets(zkUtils, zkPath)
            stream = KafkaUtils.createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Assign[String, String](
                    fromOffsets.get.keys,
                    kafkaParams,
                    fromOffsets.get)
            )
        } else {
            stream = KafkaUtils.createDirectStream[String, String](
                ssc,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams)
            )
            //Zookeeper中创建一个持久化节点
            zkUtils.createPersistentPath(zkPath, "123")
        }

        //保存offset
        stream.foreachRDD(
            rdd => saveOffsets(zkUtils, zkPath, rdd)
        )

        stream
    }

    //读取上次保存到Zookeeper的分区的offset
    def readOffsets(zkUtils: ZkUtils, zkPath: String): Option[Map[TopicPartition, Long]] = {
        val offsetsRangeStrOpt = zkUtils.readDataMaybeNull(zkPath)._1
        offsetsRangeStrOpt match {
            case Some(offsetsRangesStr) =>
                val offsets = offsetsRangesStr
                    .split(",")
                    .map(s => s.split(":"))
                    .map{m => new TopicPartition(m(0), m(1).toInt) -> m(2).toLong}.toMap
                Some(offsets)
            case None =>
                None
        }
    }

    //保存每个kafka分区的offset到Zookeeper
    def saveOffsets(zkUtils: ZkUtils, zkPath: String, rdd: RDD[_]): Unit = {
        val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val offsetRangesStr = offsetsRanges
            .map(m => s"${m.topic}:${m.partition}:${m.fromOffset}")
            .mkString(",")
        zkUtils.updatePersistentPath(zkPath, offsetRangesStr)
    }
}
