package demo

//import com.snowplowanalytics.maxmind.iplookups.IpLookups

object IPAddress {
    def main(args: Array[String]): Unit = {
        /*val spark = SparkSession.builder()
            .appName("geo")
            .enableHiveSupport()
            .getOrCreate()

        val sc = spark.sparkContext

        // HDFS 路径
        val path = "hdfs://nameservice1/third_party_data/GeoLiteCity.dat"
        //  广播文件
        sc.addFile(path)

        import spark.implicits._
        val ips = spark.sql("select id,ip from rds.beiyua_order_locations limit 5").rdd

        ips.map(m => {
            val geoFile =SparkFiles.get("GeoCity.dat")
            val id = m.get(0)
            val ip = m.get(1).toString
            // 获取广播的文件
            val ipLookups = IpLookups(
                Some(geoFile),
                None,
                None,
                None,
                true,
                100)
            val lookupResult = ipLookups.performLookups(ip)

            val city = lookupResult.ipLocation.get
            println(city)

            var locValue = (id,ip,"","","")
            locValue
        }).toDF("id","ip","country","region","city").show()
            .write.mode("overwrite").saveAsTable("test.ipLocation")

        spark.close()*/

    }

    /*def test(): Unit = {
        val ipLookups = IpLookups(
            Some("F:\\App\\Jar\\GeoLite2-City.mmdb"),
            None,
            None,
            None,
            true,
            100)
        val lookupResult = ipLookups.performLookups("59.39.129.163")

        println(lookupResult.ipLocation)
        println(lookupResult.ipLocation.get.getOrElse(null).countryName)
        println(lookupResult.ipLocation.get.getOrElse(null).regionName.get)
        println(lookupResult.ipLocation.get.getOrElse(null).city)
        println(lookupResult.ipLocation.get.getOrElse(null).latitude)
        println(lookupResult.ipLocation.get.getOrElse(null).longitude)
    }*/
}
