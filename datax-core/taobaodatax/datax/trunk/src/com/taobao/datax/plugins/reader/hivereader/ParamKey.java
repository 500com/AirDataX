package com.taobao.datax.plugins.reader.hivereader;

public final class ParamKey {
		 /*
	       * @name: ip
	       * @description: hive server ip
	       * @range:
	       * @mandatory: false
	       * @default:192.168.41.225
	       */
		public final static String ip = "hive_server_ip";
		/*
	       * @name: port
	       * @description: hive server port
	       * @range:
	       * @mandatory: false
	       * @default:10000
	       */
		public final static String port = "hive_server_port";
		/*
	       * @name: dbname
	       * @description: hive database's name
	       * @range:
	       * @mandatory: false
	       * @default:"default"
	       */
		public final static String dbname = "hive_database";
		/*
	       * @name: username
	       * @description: hive database's login name
	       * @range:
	       * @mandatory: false
	       * @default:""
	       */
		public final static String username = "username";
		/*
	       * @name: password
	       * @description: hive database's login password
	       * @range:
	       * @mandatory: false
	       * @default:""
	       */
		public final static String password = "password";
		/*
	       * @name: tables
	       * @description: tables to export data, format can support simple regex, table[0-63]
	       * @range: 
	       * @mandatory: true
	       * @default: 
	       */
		public final static String tables = "tables";
		/*
	       * @name: where
	       * @description: where clause, like 'modified_time > sysdate'
	       * @range: 
	       * @mandatory: false
	       * @default: 
	       */
		public final static String where = "where";

		/*
	       * @name: columns
	       * @description: columns to be selected, default is *
	       * @range: 
	       * @mandatory: false
	       * @default: *
	       */
		public final static String columns = "columns";
		 /*
	       * @name: concurrency
	       * @description: concurrency of the job
	       * @range: 1-10
	       * @mandatory: false
	       * @default: 1
	       */
		public final static String concurrency = "concurrency";

        /*
               * @name: sql
               * @description: self-defined sql statement
               * @range:
               * @mandatory: false
               * @default:""
               */
        public final static String sql = "hive_sql";
}
