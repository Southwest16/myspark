package hbase

import java.net.URI

import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object SparkConnectHBase {
    //读取HBase表, 转RDD/DataFrame
    def readHBase(spark: SparkSession): Unit = {
        val config = HBaseConfiguration.create()
        config.set(TableInputFormat.INPUT_TABLE, "test") //表名
        config.set(TableInputFormat.SCAN_ROW_START, "start_key") //扫描起始rowKey
        config.set(TableInputFormat.SCAN_ROW_STOP, "stop_key") //扫描终止rowKey

        //HBase表加载为RDD[(K, V)]
        val rdd = spark.sparkContext.newAPIHadoopRDD(config,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )

        //从Result中获取指定列最新版本的值
        //RDD转为RDD[Row]
        val rdd1 = rdd.map(m => {
            //获取一行查询结果
            val result: Result = m._2

            val rowKey = Bytes.toString(result.getRow) //获取row key
            val userId = Bytes.toString(result.getValue("cf".getBytes,"user_id".getBytes))
            val name = Bytes.toString(result.getValue("cf".getBytes,"name".getBytes))
            val age = Bytes.toString(result.getValue("cf".getBytes,"age".getBytes))

            Row(rowKey, userId, name, age)
        })

        //创建schema
        val schema = StructType(
          StructField("user_id", IntegerType, false) ::
          StructField("name", StringType, false) ::
          StructField("age", IntegerType, true) :: Nil)

        //RDD转为DataFrame
        val df = spark.createDataFrame(rdd1, schema)
        df.select("name", "age")
    }

    //RDD/Dataset写入HBase表
    def writeHBase(spark: SparkSession, datasetJson: Dataset[String]): Unit = {
        //假设datasetJson是一个json字符串类型("{"name":"", "age":"", "phone":"", "address":"", }")的Dataset
        val ds = spark.read.json(datasetJson)
        val rdd = ds.rdd.mapPartitions(iterator => {
            iterator.map(m => {
                //json字符串解析成JSONObject
                val json = JSON.parseObject(m.toString())
                //phone作为KeyValue的row key
                val phone = json.getString("phone")
                //以便遍历其他所有键值对
                json.remove("phone")

                //键值对字节序列
                val writable = new ImmutableBytesWritable(Bytes.toBytes(phone))
                //初始化数组, 存储JSONObject中的键值对
                val array = ArrayBuffer[(ImmutableBytesWritable, KeyValue)]()

                //JSON中的key作为Hbase表中的列名，并按字典序排序
                val jsonKeys = json.keySet().toArray
                    .map(_.toString).sortBy(x => x)

                val length = jsonKeys.length
                for (i <- 0 until length) {
                    val key = jsonKeys(i)
                    val value = json.get(jsonKeys(i)).toString
                    //KeyValue为HBase中的基本类型Key/Value。
                    //构造函数中的参数依次为：rowkey、列族、列名、值。
                    //Json对象中的每个key和其值构成HBase中每条记录的value
                    val keyValue: KeyValue = new KeyValue(
                        Bytes.toBytes(phone),  //row key
                        "cf".getBytes(), //列族名
                        key.getBytes(), //列名
                        value.getBytes()) //列的值

                    array += ((writable, keyValue))
                }
                array
            })
            //重新分区,减少保存的文件数; 展开数组中的元素; 对rowkey排序
        }).repartition(1).flatMap(x => x).sortByKey()

        val config = HBaseConfiguration.create()
        config.set(TableInputFormat.INPUT_TABLE, "test") //表名

        val job = Job.getInstance(config)
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])

        //持久化到HBase表
        rdd.saveAsNewAPIHadoopFile("/tmp/test",
            classOf[ImmutableBytesWritable],
            classOf[KeyValue],
            classOf[HFileOutputFormat2],
            job.getConfiguration)

//        val path = new Path("hdfs://nameservice1/test/*")
//        val fs = FileSystem.get(new URI("hdfs://nameservice1/"), new Configuration())
//        if (fs.exists(path)) {
//            fs.delete(path, true)
//        }
//
//        rdd.saveAsNewAPIHadoopFile("/test",
//            classOf[ImmutableBytesWritable],
//            classOf[KeyValue],
//            classOf[HFileOutputFormat2], config)
//
//        val load = new LoadIncrementalHFiles(config)
//        load.doBulkLoad(new Path("/test"),
//            connection.getAdmin,
//            table,
//            connection.getRegionLocator(tableName))
    }
}