package com.taobao.datax.plugins.reader.postgrereader;

public final class ParamKey {
	/*
       * @name: dbname
       * @description: Oracle database name
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String dbname = "dbname";
	/*
       * @name: username
       * @description:  Oracle database login username
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String username = "username";
	/*
       * @name: password
       * @description: Oracle database login password
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String password = "password";
	/*
       * @name: schema
       * @description: Oracle database schema
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String schema = "schema";
	 /*
       * @name: ip
       * @description: Oracle database ip address
       * @range:
       * @mandatory: true
       * @default:
       */
	public final static String ip = "ip";
	/*
       * @name: port
       * @description: Oracle database port
       * @range:
       * @mandatory: true
       * @default: 5432
       */
	public final static String port = "port";
	/*
       * @name: tables
       * @description: tables to be exported
       * @range: 
       * @mandatory: true
       * @default: 
       */
	public final static String tables = "tables";
	
	/*
       * @name: columns
       * @description: columns to be selected
       * @range: 
       * @mandatory: false
       * @default: *
       */
	public final static String columns = "columns";
	
	/*
       * @name: where
       * @description: where clause, like 'gmtdate > trunc(sysdate)'
       * @range: 
       * @mandatory: false
       * @default: 
       */
	public final static String where = "where";		
	/*
       * @name: sql
       * @description: self-defined sql statement
       * @range: 
       * @mandatory: false
       * @default: 
       */
	public final static String sql = "sql";
	/*
       * @name: encoding
       * @description: oracle database encode
       * @range: UTF-8|GBK|GB2312
       * @mandatory: false
       * @default: UTF-8
       */
	public final static String encoding = "encoding";
	 /*
       * @name:concurrency
       * @description:concurrency of the job
       * @range:1-100
       * @mandatory: false
       * @default:1
       */
	public final static String concurrency = "concurrency";
    /*
       * @name:fetchSize
       * @description:jdbc query fetch size
       * @mandatory: false
       * @default:1000
       */
    public final  static  String fetchSize = "fetch_size";
}
