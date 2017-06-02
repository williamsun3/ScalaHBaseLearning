
// Library files.
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{
  HBaseConfiguration,
  HColumnDescriptor,
  HTableDescriptor}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * Created by peterbugaj on 2017-06-01.
  */
object DataGenerator {

  val tblName = "StarbucksTable";
  
  val family1 = "coffee";
  val family2 = "latte";

  def main(args: Array[String]) {
    
    // Configure the connection.
    val conf : Configuration = HBaseConfiguration.create();
    
    val ZOOKEEPER_QUORUM =
      "cdn-dataproc-custom-m" + "," +
      "cdn-dataproc-custom-w0" + "," +
      "cdn-dataproc-custom-w1";
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
    
    val admin = new HBaseAdmin(conf);

    // Create the table
    if (admin.tableExists(tblName)) {
      admin.disableTable(tblName);
      admin.deleteTable(tblName);
    }

    val tableDesc = new HTableDescriptor(Bytes.toBytes(tblName));
    val columnFamilyDesc1 = new HColumnDescriptor(Bytes.toBytes(family1));
    tableDesc.addFamily(columnFamilyDesc1);
    val columnFamilyDesc2 = new HColumnDescriptor(Bytes.toBytes(family2));
    tableDesc.addFamily(columnFamilyDesc2);

    admin.createTable(tableDesc);
    
    // Add some data.
    val table = new HTable(conf, tblName);
    val rand = scala.util.Random;
    
    List.range(0, 1000).foreach(batchCount => {
      
      val batch = List.range(0, 10000).map(number => {
        new Put(Bytes.toBytes("" + ((batchCount * 1000) + number))).
          add(
            Bytes.toBytes(family1),
            Bytes.toBytes("store"),
            Bytes.toBytes("" + rand.nextInt(1000))).
          add(
            Bytes.toBytes(family1),
            Bytes.toBytes("article"),
            Bytes.toBytes("" + rand.nextInt(1000))).
          add(
            Bytes.toBytes(family2),
            Bytes.toBytes("sales"),
            Bytes.toBytes("" + rand.nextInt(10000))).
          add(
            Bytes.toBytes(family2),
            Bytes.toBytes("quantity"),
            Bytes.toBytes("" + rand.nextInt(100)));
      });
      
      table.put(batch.asJava);
    });
  }
}
