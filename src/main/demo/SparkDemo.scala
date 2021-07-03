package demo

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage
import org.apache.spark.storage.StorageLevel
//import org.bson.BSONObject
import org.json.JSONObject

object SparkDemo {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
                .appName("Spark Demo")
                .master("local[4]")
                .getOrCreate()
        import spark.implicits._

        sparkSQL(spark)
        //rdd(spark)
        //mongodb(spark)

//        val rdd = spark.emptyDataFrame.repartition($"")
//        spark.sparkContext.broadcast(spark.emptyDataFrame.rdd.persist(StorageLevel.OFF_HEAP))

        spark.close()
    }


    def etl(): Unit = {
        val extractFields: Seq[Row] => Seq[(String, Int)] = {
            (rows: Seq[Row]) => {
                var fields = Seq[(String, Int)]()
                rows.map(row => {
                    fields = fields :+ (row.getString(2), row.getInt(4))
                })
                fields
            }
        }

        /*val extractFields: Seq[Row] => Seq[(String, Int)] = {
            (rows: Seq[Row]) => {
                rows.map(row => (row.getString(2), row.getInt(4))).toSeq
            }
        }*/
    }


    def rdd(spark: SparkSession): Unit = {
        var acc = spark.sparkContext.longAccumulator("acc")
        val rdd = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 100000000), 4)
                .persist(StorageLevel.MEMORY_AND_DISK)
        val rdd1 = rdd.map(m => m + 1)
        val rdd2 = rdd.filter(f => f % 2 == 0)

        spark.sparkContext.textFile("hdfs://master-1:9000/file/test.txt", 3)
                .checkpoint()

        import spark.implicits._
        spark.read.parquet("").as[Person]
        Seq(1, 2, 3, 4, 100000000).toDS()

        /**
         * 将任何必须的资源上传到分布式缓存中。如果是在本地使用的资源，则为下游代码设置适当的配置以正确处理这资源。
         * 为ApplicationMaster设置一个container启动上下文。
         *
         * 将应用程序必需的Spark jar文件和应用程序jar文件上传到远程文件系统（例如HDFS），
         * 并将这些文件设置为ApplicationMaster的本地资源。
         *
         *
         **/


        //    spark.sparkContext.textFile("F:/rdd.txt").flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
        //    val rdd         = spark.sparkContext.textFile("F:\\rdd.txt")
        //    val flatMap     = rdd.flatMap(line => line.split(","))
        //    val map         = flatMap.map(word => (word, 1))
        //    val reduceByKey = map.reduceByKey(_ + _)
        //    val foreach     = reduceByKey.foreach(println)
    }

    //Spark SQL窗口函数
    def sparkSQLWindowsFunctions(spark: SparkSession): Unit = {
        val seq = Seq((1, "a"), (2, "a"), (3, "a"), (1, "b"), (2, "b"), (3, "b"))
        import spark.implicits._
        spark.createDataset(seq)
                .toDF("id", "category")
                .select($"*", sum("id").over(
                    Window.partitionBy('category).orderBy('id).rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
                ).as("rn"))
                .show()
    }

    def localSpark2Hive(spark: SparkSession): Unit = {
        /** Spark本地读远程 Hive */
        val spark = SparkSession
            .builder()
            .appName("local application")
            .master("local")
            .config("hive.metastore.uris", "thrift://bigdata04:9083,thrift://bigdata05:9083")
            .enableHiveSupport()
            .getOrCreate()
        val start = System.currentTimeMillis()
        import spark.implicits._
        spark.read.table("third.mobile_info")
            .select($"user_mobile",
                $"mobile_net_time",
                lit("13012345678").substr(1, 7))
            .show()
        //spark.sql("select user_mobile,mobile_net_time from third.mobile_info").show()
        val end = System.currentTimeMillis()
        println(end-start)

        /*spark.read
            .format("jdbc")
            .option("driver", "org.apache.hive.jdbc.HiveDriver")
            .option("url", "jdbc:hive2://host:10000")
            .option("dbtable", "ods.exception_stats")
            .load()
            .printSchema()*/
        //print(spark.catalog.tableExists("third", "call_record"))
        spark.close()
    }

    def sparkSQL(spark: SparkSession): Unit = {
        import spark.implicits._
        /*val df: Dataset[Row] = spark.read.table("").repartition(1)
        df.persist(StorageLevel.MEMORY_ONLY)
        val rdd: RDD[Int] = spark.sparkContext.parallelize(Seq(0, 1)).repartition(1)
        rdd.cache()
        spark.sparkContext.broadcast(rdd)*/


        /*val rdd = spark.sparkContext.makeRDD(Seq(new JSONObject("{'data':17500}").toString))
        println(spark.read.json(rdd).first().length)*/



        //    val jsonStr = Map("1000" -> "{'name':'阿木','date':2}")
            val jsonStr = Map(
              "1000" -> "{'name':'zhangsan','age':null}",
              "2000" -> "{'name':'zhangsan','age':10}",
              "4000" -> "{'name':'zhangsan','age':10}",
              "5000" -> "{'name':'lisi','age':20}",
              "6000" -> "{'name':'lisi','age':20}",
              "7000" -> "{'name':'wangwu','age':20}",
              "8000" -> "{'name':'zhaoliu','age':50}",
              "9000" -> "{'name':'zhaoliu','age':40}",
              "10000" -> "{'name':'zhaoliu','age':20}"
            )
            val jsonRDD = spark.sparkContext.makeRDD(jsonStr.values.toSeq).cache()
            spark.read.json(jsonRDD).select($"name", split($"phone", ",").alias("phone")).where($"phone" < 2)
              .show(false)

            /*spark.read.json(jsonRDD)
              .groupBy($"name", $"age").agg("age" -> "count").withColumnRenamed("count(age)", "cnt")
              .select($"name", $"age", row_number().over(Window.partitionBy("name").orderBy($"cnt".desc)).alias("rn"))
              .where($"rn".<=(2)).show()*/




        /*spark.read.json(jsonRDD).createOrReplaceTempView("tmp")
        spark.sql("select name,age from tmp").show(false)*/


        //    spark.catalog.refreshTable("demo.table")
        //    spark.sql("refresh table demo.table")
        //    spark.sql("select auth_type,etl_ts from demo.table limit 1").write.mode("append").format("parquet").saveAsTable("demo.gxb_new")

        /*val people = spark.read.parquet("").as[Person]
        val names = people.map(_.name)
        val ageCol = people("age") + 10
        Seq(1, 2, 3).toDS().show()*/

        /*val df = spark.createDataFrame(Seq(("2018-01-22 00:00:00", "Spark Hadoop Hbase Hive Kafka Storm", 3)))
          .withColumnRenamed("_1", "date").withColumnRenamed("_2", "skill").withColumnRenamed("_3", "num")
            df.explain()//Prints the physical plan to the console for debugging purposes.
            println(df.dtypes(0))// Returns all column names and their data types as an array.
            println(df.columns(0))//Returns all column names as an array.
            println(df.isStreaming)
            df.show(1, false)
            df.na.drop().show(false)
            df.sort($"date".desc).show()
            println(df.apply("date"))
            df.as("df_")
            df.select(expr("num + 1").as[Int]).show(false)
            df.filter("num > 1").show(false)
            df.groupBy($"num").avg().show(false)
            df.groupBy($"num").agg(Map("num" -> "max")).show(false)
            df.select('name, explode(split('age, " ")).as("age"))*/


        /*val jsonStr = Map("1000" -> "{'name':'阿木','phone':'18300000000'}")
        val jsonRDD = spark.sparkContext.makeRDD(jsonStr.get("1000").toSeq)
        spark.read.json(jsonRDD).show(false)*/

        /*val t = Tuple2(id, project_id)
        val rdd = spark.sparkContext.makeRDD(Seq(t))
        rdd.toDS().withColumnRenamed("_1", "id").withColumnRenamed("_2", "project_id").show()
        println(rdd.toDS())*/


        /*
        //方法四
        val df = spark.createDataFrame(jsonStr.toSeq).select($"_1", json_tuple(($"_2").cast("string"), "$.name", "$.phone")).show(false)

        //方法三
        spark.createDataFrame(jsonStr.toSeq)
          .map(m => {
            val json = new JSONArray(m.get(1))
            val len = json.length()
            for(i <- 0 until len){

            }
          }).show(false)

        //方法二
        val jsonRDD = spark.sparkContext.makeRDD(jsonStr.get("1000").toSeq)
        spark.read.json(jsonRDD).show(false)

        //方法一
          val schema = StructType(
            Array(
              StructField("name", StringType, true),
              StructField("phone", StringType, true)
            )
          )
          val df = spark.createDataFrame(jsonStr.toSeq).select($"_1",
            explode(
              split(
                substring_index(
                  substring_index(
                    regexp_replace($"_2", "},", "}^^^"),
                    "[", -1),
                  "]", 1),
                "\\^\\^\\^")
            ).as("_2")
          )
        df.select($"_1",
          from_json($"_2", schema).getField("name").as("contacts_name"),
          from_json($"_2", schema).getField("phone").as("contacts_phone")
        ).show(false)*/


        /*
        //时间函数
        spark.createDataFrame(Seq(("2018-01-22 00:00:00", 2, 3))).select("date").withColumn("date", substring($"date", 0, 7)).withColumn("date", date_sub(concat(substring($"date", 0, 8), lit("15 00:00:00")), 30))
        LocalDate.parse("2018-01-22 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")).minusMonths(1)*/
    }

    def sparkDemo(spark: SparkSession): Unit = {
        val sc = spark.sparkContext

        /*val input = sc.parallelize(1 to 10000000, 42).map(x => (x % 42, x))
        val rdd1 = input.groupByKey().mapValues(_.sum)
        val rdd2 = input.reduceByKey(_ + _).sortByKey().collect()

        input.take(50).foreach(println)
        rdd1.foreach(println)
        rdd2.foreach(println)*/

        val input = sc.parallelize(1 to 15, 42)
            .keyBy(_ % 10)//将_ % 10作为key, 1 to 15作为value
        val combined = input.combineByKeyWithClassTag(
            (x: Int) => Set(x / 10),    //`createCombiner`, which turns a V into a C (e.g., creates a one-element list)
            (s: Set[Int], x: Int) => s + x / 10,    //`mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
            (s1: Set[Int], s2: Set[Int]) => s1 ++ s2)   //`mergeCombiners`, to combine two C's into a single one

        combined.foreach(println)
    }

    def mongodb(spark: SparkSession): Unit = {
        /*val conf = new Configuration()
        conf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")
        conf.set("mongo.auth.uri", "mongodb://username:host/admin")
        conf.set("mongo.input.uri", "mongodb://host:3717/admin")
        val fClass = classOf[com.mongodb.hadoop.MongoInputFormat]
        val kClass = classOf[Object]
        val vClass = classOf[BSONObject]
        val rdd = spark.sparkContext.newAPIHadoopRDD(conf, fClass, kClass, vClass).repartition(1)
        println(rdd.count())*/

    }


    case class Person(name: String, age: String)
}
