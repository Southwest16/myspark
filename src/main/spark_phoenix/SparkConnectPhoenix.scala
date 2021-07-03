package spark_phoenix

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SparkConnectPhoenix {
    //RDD写入Phoenix表
    def writeRDD2Phoenix(spark: SparkSession): Unit = {
        //保存RDD中的数据到Phoenix表
        val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))
        spark.sparkContext
            .parallelize(dataSet)
            .saveToPhoenix(
                "TEST", //表名
                Seq("ID","COL1","COL2"), //列命名
                zkUrl = Some("host:2181") //Zookeeper URL
            )
    }

    //DataFrame写入Phoenix表
    def writeDF2Phoenix(spark: SparkSession): Unit = {
        val df = spark.read.table("db.table")

        //方式一
        df.saveToPhoenix(Map("table" -> "TEST", "zkUrl" -> "host:2181"))

        //方式二
        df.write
                .format("org.apache.phoenix.spark")
                .mode("overwrite")
                .option("table", "OUTPUT_TABLE")
                .option("zkUrl", "host:2181")
                .save()
    }

    //加载Phoenix表为RDD
    def readPhoenixAsDF(spark: SparkSession): Unit = {
        //方法一：使用数据源API加载Phoenix表为一个DataFrame
        val df1 = spark.read
            .format("org.apache.phoenix.spark")
            .options(Map("table" -> "TEST", "zkUrl" -> "host:2181"))
            .load()

        //方法二：使用Configuration对象加载Phoenix表为一个DataFrame
        val conf = new Configuration()
        conf.set("hbase.zookeeper.quorum", "hostname:2181")
        val df2 = spark.sqlContext.phoenixTableAsDataFrame(
            "test", //表名
            Seq("NAME", "AGE"), //指定要加载的列名
            predicate = Some("PHONE = 13012340000"), //可设置where条件
            conf = conf)
    }

    //加载Phoenix表为DataFrame
    def readPhoenixAsRDD(spark: SparkSession): Unit = {
        //使用Zookeeper URL加载Phoenix表为一个RDD
        val rdd: RDD[Map[String, AnyRef]] = spark.sparkContext.phoenixTableAsRDD(
            "TEST",
            Seq("NAME", "AGE"),
            predicate = Some("PHONE = 13012340000"),
            zkUrl = Some("hostname:2181") //Zookeeper URL来连接Phoenix
        )
        rdd.map(_.get(""))
    }
}
