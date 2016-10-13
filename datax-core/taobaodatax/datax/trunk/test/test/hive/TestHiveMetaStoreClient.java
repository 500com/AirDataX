package test.hive;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hive.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.util.Iterator;

/**
 * Created by zhuhq on 2015/11/27.
 */
public class TestHiveMetaStoreClient {
    @Test
    public void getTableTest() throws TException {
       // HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient()
        TTransport transport = new TSocket("192.168.41.225", 9083);
        TProtocol protocol = new TBinaryProtocol(transport);

        ThriftHiveMetastore.Client metaStoreClient = new ThriftHiveMetastore.Client(protocol);
        try {
            System.out.println("open");
            transport.open();
            Table table = metaStoreClient.get_table("DEFAULT", "dw_mbr_userinfo_20151116");
            StorageDescriptor sd = table.getSd();
            System.out.println(sd.getLocation());
            SerDeInfo serDeInfo = sd.getSerdeInfo();
            SkewedInfo skewedInfo = sd.getSkewedInfo();
            System.out.println(table.getViewExpandedText());
        }finally {
            transport.close();
        }

    }
}
