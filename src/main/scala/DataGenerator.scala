// Library files.
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration

/**
  * Created by peterbugaj on 2017-06-01.
  */
object DataGenerator {

  def main(args: Array[String]) {
    
    val conf : Configuration = HBaseConfiguration.create();
    
    val ZOOKEEPER_QUORUM =
      "cdn-dataproc-custom-m" + "," +
      "cdn-dataproc-custom-w0" + "," +
      "cdn-dataproc-custom-w1";
    println ("hbase.zookeeper.quorum" + ZOOKEEPER_QUORUM);
    
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
    
    val admin = new HBaseAdmin(conf)

  }
}
