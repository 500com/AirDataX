package com.taobao.datax.plugins.reader.hdfsreader;

public final class ParamKey {
	/*
	 * @name: ugi
	 * @description: HDFS login account, e.g. 'username, groupname(groupname...),#password
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String ugi = "hadoop.job.ugi";
	
	/*
	 * @name: hadoop_conf
	 * @description: hadoop-site.xml path
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String hadoop_conf = "hadoop_conf";
	
	/*
	 * @name: dir
	 * @description: hdfs path, format like: hdfs://ip:port/path, or file:///home/taobao/ or hive
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String dir = "dir";
	/*
	 * @name: fieldSplit
	 * @description: how to sperate a line
	 * @range:
	 * @mandatory: false 
	 * @default:\t
	 */
	public final static String fieldSplit = "field_split";
	/*
	 * @name: encoding 
	 * @description: hdfs encode
	 * @range:UTF-8|GBK|GB2312
	 * @mandatory: false 
	 * @default:UTF-8
	 */
	public final static String encoding = "encoding";
	/*
	 * @name: bufferSize
	 * @description: how large the buffer
	 * @range: [1024-4194304]
	 * @mandatory: false 
	 * @default: 4096
	 */
	public final static String bufferSize = "buffer_size";

	/*
       * @name: nullString
       * @description: replace the nullstring to null
       * @range: 
       * @mandatory: false
       * @default: \N
       */
	public final static String nullString = "null_string";
	/*
	 * @name: ignoreKey
	 * @description: ingore key
	 * @range: true|false
	 * @mandatory: false 
	 * @default: true
	 */		
	public final static String ignoreKey = "ignore_key";
	/*
	 * @name: colFilter
	 * @description: how to filter column
	 * @range: 
	 * @mandatory: false 
	 * @default: 
	 */		
	public final static String colFilter = "col_filter";

	 /*
       * @name:concurrency
       * @description:concurrency of the job
       * @range:1-100
       * @mandatory: false
       * @default:1
       */
	public final static String concurrency = "concurrency";
    /*
	 * @name: hiveWarehouseDir
	 * @description:hive metastore warehouse dir
	 * @range:
	 * @mandatory: false
	 * @default:/user/hive/warehouse
	 */
    public final static String hiveWarehouseDir="hive_warehouse_dir";
    /*
    * @name: hiveWarehouseThriftServer
    * @description:hive metastore thrift server ip
    * @range:
    * @mandatory: false
    * @default:
    */
    public final static String hiveMetastoreThriftServer = "hive_metastore_thrift_server";
    /*
    * @name: hiveWarehouseThriftPort
    * @description:hive metastore thrift server port
    * @range:
    * @mandatory: false
    * @default:9083
    */
    public final static String hiveMetastoreThriftPort = "hive_metastore_thrift_port";
    /*
   * @name: hiveServerIp
   * @description:hive server ip
   * @range:
   * @mandatory: false
   * @default:
   */
    public final static String hiveServerIp = "hive_server_ip";
    /*
    * @name: hiveServerPort
    * @description:hive server port
    * @range:
    * @mandatory: false
    * @default:10000
    */
    public final static String hiveServerPort = "hive_server_port";
    /*
      * @name:hiveTableNames
      * @description:hive table  table names
      * @mandatory: false
      * @default:null
      */
    public final static String hiveTableName = "hive_table_name";

    /*
       * @name:hivePartitionNames
       * @description:hive table partition names
       * @mandatory: false
       * @default:""
       */
    public final static String hivePartitionNames = "hive_partition_names";

    /*
     * @name:hivePartitionValues
     * @description:hive table partition values
     * @mandatory: false
     * @default:""
     */
    public final static String hivePartitionValues = "hive_partition_values";

    /*
    * @name:hiveDatabase
    * @description:hive database name
    * @mandatory: false
    * @default:"default"
    */
    public final static String hiveDatabase = "hive_database";
    /*
   * @name:hiveFsAddress
   * @description:hdfs://s-master:9000/
   * @mandatory: true
   * @default:
   */
    public final static String hiveFsAddress = "hive_fs_address";
    /*
   * @name:hiveSql
   * @description: query sql
   * @mandatory: false
   * @default:""
   */
    public final static String hiveSql = "hive_sql";
}

