package spark_mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkMySQL {
    //DataFrame写入MySQL
    def writeMySQL(spark: SparkSession): Unit = {
        val host = "hostname" //MySQL服务器地址
        val port = "3306" //端口号
        val userName = "userName" //用户名
        val password = "password" //访问密码
        val db = "dbName" //库名
        val jdbcUrl = s"jdbc:mysql://${host}:${port}/${db}" //jdbc url

        import java.util.Properties
        val prop = new Properties()
        prop.put("user", userName)
        prop.put("password", password)

        spark.read.table("db.test")
                .coalesce(10) //调节写入并行度(增加并行度要用repartition)
                .write
                .mode("append") //追加写入()
                .jdbc(jdbcUrl, "test", prop)
    }

    //通过设置分区数并行读取MySQL
    def readByIntegralColumn(spark: SparkSession): Unit = {
        //只适用于数字类型的字段进行分区
        val options = Map(
            "url" -> "jdbc:mysql://host:3306/dbName",
            "dbtable" -> "tableName",
            "user" -> "userName",
            "password" -> "passwd",
            "driver" -> "com.mysql.jdbc.Driver",
            "partitionColumn" -> "id", //the name of a column of integral type that will be used for partitioning.
            "lowerBound" -> "1", //the minimum value of `columnName` used to decide partition stride.
            "UpperBound" -> "400000", //the maximum value of `columnName` used to decide partition stride.
            "numPartitions" -> "10" //the number of partitions.
        )

        spark
            .read
            .format("jdbc")
            .options(options)
            .load()
            .write
            .saveAsTable("dbName.tableName")
    }

    //通过设置范围区间并行读取MySQL
    def readByRange(spark: SparkSession): Unit = {
        val prop = new Properties()
        prop.put("user", "userName")
        prop.put("password", "passwd")

        val url = "jdbc:mysql://host:3306//dbName"
        val table = "tableName"

        //predicates参数就相当于在where表达式中添加范围, 有几个范围就有几个分区并行读取
        val predicates = Array[String](
            "created_at < '2018-08-01 00:00:00'",
            "created_at >= '2018-08-01 00:00:00' && created_at < '2018-10-01 00:00:00'",
            "created_at >= '2018-10-01 00:00:00'")

        spark
            .read
            .jdbc(url, table, predicates, prop)
            .write
            .saveAsTable("dbName.tableName")
    }
}
