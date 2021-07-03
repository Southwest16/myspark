package hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

object HBaseAPI {
    def main(args: Array[String]): Unit = {
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "hostname")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        val connection = ConnectionFactory.createConnection(config)
        val admin = connection.getAdmin

        val listTables = admin.listTables()
        listTables.foreach(println)

        // Create table
        val descriptor = new HTableDescriptor(TableName.valueOf("taf"))
        descriptor.addFamily(new HColumnDescriptor("cf1"))
        descriptor.addFamily(new HColumnDescriptor("cf2"))
        admin.createTable(descriptor)

        val table = connection.getTable(TableName.valueOf("TEST.KAFKAOFFSET"))

        //向表中put数据
        val theput = new Put(Bytes.toBytes("rowkey1"))
        theput.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("id1"),Bytes.toBytes("one"))
        table.put(theput)

        //从表中get数据
        val theget = new Get(Bytes.toBytes("app_logs-1"))
        val result = table.get(theget)
        val value = result.value()
        println(Bytes.toLong(value))


        //scan表数据
        val scan = new Scan()
        val rs = table.getScanner(scan).iterator()
        var i = 0
        while (rs.hasNext) {
            i += 1
            val next = rs.next()
            for (kv <- next.rawCells()) {
                print(Bytes.toString(kv.getValue) + "  ")
            }
            println("\n")
        }

    }
}
