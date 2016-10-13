package com.taobao.datax.plugins.writer.postgrewriter;

/**
 * Created by yuanw on 2015/11/5.
 */
public final class ParamKey {
    /*
     * @name: ip
     * @description:  database ip address
     * @range:
     * @mandatory: true
     * @default:
     */
    public final static String ip = "ip";
    /*
      * @name: port
      * @description: Postgre database port
      * @range:
      * @mandatory: true
      * @default:5432
      */
    public final static String port = "port";
    /*
      * @name: dbname
      * @description: Postgre database name
      * @range:
      * @mandatory: true
      * @default:
      */
    public final static String dbname = "dbname";
    /*
      * @name: username
      * @description: Postgre database login username
      * @range:
      * @mandatory: true
      * @default:
      */
    public final static String username = "username";
    /*
      * @name: password
      * @description: Postgre database login password
      * @range:
      * @mandatory: true
      * @default:
      */
    public final static String password = "password";
    /*
      * @name: table
      * @description: table to be dumped data into
      * @range:
      * @mandatory: true
      * @default:
      */
    public final static String table = "table";

    /*
      * @name: encoding
      * @description:
      * @range: UTF-8|GBK|GB2312
      * @mandatory: false
      * @default: UTF-8
      */
    public final static String encoding = "encoding";
    /*
     * @name: pre
     * @description: execute sql before dumping data
     * @range:
     * @mandatory: false
     * @default:
     */
    public final static String pre = "pre";
    /*
     * @name: post
     * @description: execute sql after dumping data
     * @range:
     * @mandatory: false
     * @default:
     */
    public final static String post = "post";

    /*
     * @name: limit
     * @description: error limit
     * @range: [0-65535]
     * @mandatory: false
     * @default: 0
     */
    public final static String limit = "limit";
    /*
     * @name: set
     * @description:
     * @range:
     * @mandatory: false
     * @default:
     */
    public final static String set = "set";

    /*
	 * @name: fieldSplit
	 * @description: how to sperate a line
	 * @range:
	 * @mandatory: false
	 * @default:\001
	 */
    public final static String fieldSplit = "field_split";

    /*
     * @name:concurrency
     * @description:concurrency of the job
     * @range:1-100
     * @mandatory: false
     * @default:1
     */
    public final static String concurrency = "concurrency";
}
