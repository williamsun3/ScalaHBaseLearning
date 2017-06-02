
// Library files.
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{
  HBaseConfiguration,
  HColumnDescriptor,
  HTableDescriptor}
import org.apache.hadoop.conf.Configuration

/**
  * Created by peterbugaj on 2017-06-01.
  */
object DataGenerator {

  val tblName = "StarbucksTable";

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
    val coffeeColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("coffee"));
    tableDesc.addFamily(coffeeColumnFamilyDesc);
    val latteColumnFamilyDesc = new HColumnDescriptor(Bytes.toBytes("latte"));
    tableDesc.addFamily(latteColumnFamilyDesc);

    admin.createTable(tableDesc);
    
    // Add some data.
    val table = new HTable(conf, tblName);
    List.range(1, 1000).foreach(number => {
      
    });
  }
}
