package com.taobao.datax.plugins.writer.oraclewriterjdbc;

public final class ParamKey {
	/*
	 * @name: dbname
	 * @description: Oracle database dbname
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String dbname = "dbname";
	
	/*
       * @name: schema
       * @description: Oracle database schema
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String schema = "schema";
	
	/*
	 * @name: table
	 * @description: table to be dumped data into
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String table = "table";
    /**
     *
     *  /*
     * @name: columns
     * @description: table columns
     * @range:
     * @mandatory: true
     * @default:""
     */
    public final static String columns = "columns";

    /*
	 * @name: username
	 * @description: oracle database login username
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String username = "username";
	
	/*
	 * @name: password
	 * @description: oracle database login password
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String password = "password";

	/*
	 * @name: dtfmt
	 * @description: oracle time format
	 * @range: 
	 * @mandatory: true
	 * @default:yyyyMMddhhmmss
	 */
	public final static String dtfmt = "dtfmt";

	/*
	 * @name: pre
	 * @description: execute pre sql before writing data .
	 * @range:
	 * @mandatory: true
	 * @default:
	 */
	public final static String pre = "pre";
	/*
	 * @name: post
	 * @description: execute post sql after writing data .
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String post = "post";
	/*
	 * @name: encoding
	 * @description: oracle encode
	 * @range: UTF-8|GBK|GB2312
	 * @mandatory: false
	 * @default: UTF-8
	 */
	public final static String encoding = "encoding";
	/*
	 * @name: colorder
	 * @description: order of columns
	 * @range: col1,col2...
	 * @mandatory: false
	 * @default:
	 */
	public final static String colorder = "colorder";
	
	/*
	 * @name: limit
	 * @description: limit amount of errors
	 * @range:
	 * @mandatory: false
	 * @default: 0
	 */
	public final static String limit = "limit";
	
	 /*
       * @name:concurrency
       * @description:concurrency of the job
       * @range:1-100
       * @mandatory: false
       * @default:1
       */
	public final static String concurrency = "concurrency";
    /*
     * @name: ip
     * @description: Mysql database ip address
     * @range:
     * @mandatory: true
     * @default:localhost
     */
    public final static String ip = "ip";
    /*
      * @name: port
      * @description: Mysql database port
      * @range:
      * @mandatory: true
      * @default:3306
      */
    public final static String port = "port";
    /*
     * @name: url
     * @description: jdbc url
     * @range:
     * @mandatory: true
     * @default:""
     */
    public final static String url = "url";
    /*
     * @name: batchsize
     * @description: execute batch size
     * @range:
     * @mandatory: true
     * @default:500
     */
    public final static String batchsize = "batchsize";
}
