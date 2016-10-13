package test.hive;

import com.taobao.datax.plugins.common.DFSUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;

/**
 * Created by zhuhq on 2016/8/12.
 */
public class TestHiveParquetSchema {
    @Test
    public void  printSchema() throws Exception{
        Configuration conf = DFSUtils.getConf("hdfs://node-1:8020/user/hive/warehouse", "hdfs,hadoop#hadoop", "d:\\work\\datax\\hadoopConf\\core-site-cdh.xml");
        Path p = new Path("hdfs://node-1:8020/user/hive/warehouse/tpcds_bin_partitioned_parquet_100.db/customer_address/000000_0");
        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, p);
        StringBuilder sb = new StringBuilder("");
        readFooter.getFileMetaData().getSchema().writeToStringBuilder(sb,"    ");
        System.out.println("schema:" + sb.toString());
    }
}
