/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package com.taobao.datax.plugins.reader.postgrereader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.LineSender;
import com.taobao.datax.common.plugin.MetaData;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Reader;
import com.taobao.datax.common.plugin.Splitter;
import com.taobao.datax.plugins.common.DBResultSetSender;
import com.taobao.datax.plugins.common.DBSource;
import com.taobao.datax.plugins.common.DBUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

//import com.taobao.datax.plugins.common.OracleTnsAssist;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import java.io.IOException;

public class PostgreReader extends Reader {
	/*public static final String POSTGRE_READER_DB_POOL_KEY = "POSTGRE_DB_POOL_KEY";*/


	private String username = "";

	private String password = "";

	private String ip;

	private String port = "1521";

	private String dbname;

	private String sql;

	private String charset = "utf-8";
    private int fetchSize = 1000;

//	private String tnsname;
	
	private int concurrency;
	
//	private int splitMode;

	private String connectKey;

	private static final Set<String> supportEncode = new HashSet<String>() {
		{
			add("utf-8");
			add("gbk");
			add("gb2312");
		}
	};

	private Logger logger = Logger.getLogger(PostgreReader.class);

	@Override
	public int init() {
		this.sql = param.getValue(ParamKey.sql, "").trim();
		
		//since  jdbc driver must ensure sql cannot end with ';'
		if (this.sql.endsWith(";")) {
			this.sql = this.sql.substring(0, this.sql.lastIndexOf(';'));
			param.putValue(ParamKey.sql, this.sql);
		}
		
		this.charset = param
				.getValue(ParamKey.encoding, "utf-8")
				.toLowerCase().trim();
		if (!isSupportEncode(charset)) {
			throw new DataExchangeException("encoding error");
		}

		this.ip = param.getValue(ParamKey.ip, "");
		this.port = param.getValue(ParamKey.port, this.port);
		this.username = param.getValue(ParamKey.username, this.username);
		this.password = param.getValue(ParamKey.password, this.password);
		this.dbname = param.getValue(ParamKey.dbname);
		this.concurrency = param.getIntValue(ParamKey.concurrency, 1);
		/*this.connectKey = param.getValue(POSTGRE_READER_DB_POOL_KEY, "postgre_pool_key");*/
        this.fetchSize = param.getIntValue(ParamKey.fetchSize,fetchSize);
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int prepare(PluginParam param) {
	/*	if (!StringUtils.isBlank(this.ip) && !StringUtils.isBlank(this.port)) {
			*//* User defined ip & port *//*
			this.connectKey = DBSource.genKey(this.getClass(), this.ip, this.port, this.dbname, username, password);
			DBSource.register(this.connectKey, this.createProperties());
			//this.connection = DBSource.getConnection(this.connectKey);
		}
		*//*if (null == this.connection) {
			String msg = "PostgreReader try to connect to database failed .";
			logger.error(msg);
			throw new DataExchangeException(msg);
		}
        try {
            logger.info("set postgre connection autocommit false");
            this.connection.setAutoCommit(false);
        } catch (SQLException e) {
           logger.error("set connection auto commit false error",e);
        }*//*
        param.putValue(POSTGRE_READER_DB_POOL_KEY, this.connectKey);*/
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public List<PluginParam> split(PluginParam param) {
		List<PluginParam> params = null;
		if (!StringUtils.isBlank(this.sql)) {
			/* user-defined sql */
			params = super.split(param);
		} else {
            logger.info("PostgreReader start split mechanism .");
            Splitter spliter = new PostgreReaderTableSplitter(param);
			spliter.init();
			params = spliter.split();
		}
		
		String sql = params.get(0).getValue(ParamKey.sql);
		MetaData m = null;
        Connection splitConn = null;
		try {
            splitConn =getCon();
            logger.info("PostgreReader genMetaData.");
            //最多取一条数据 以减少io
			m = DBUtils.genMetaData(splitConn, String.format("select * from (%s) postgre_reader_limit_table limit 1", sql));
            logger.info("PostgreReader setMetaData.");
			param.setMyMetaData(m);

		} catch (SQLException e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e);
		}finally {
            if(splitConn != null) {
                try {
                    splitConn.close();
                    splitConn = null;
                } catch (Exception ignore) {
                    logger.error("close connection error:",ignore);
                }

            }
        }

		return params;
	}

	@Override
	public int connect() {
		/*connection = DBSource.getConnection(this.connectKey);*/

		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int startRead(LineSender lineSender) {
		DBResultSetSender proxy = DBResultSetSender.newSender(lineSender);
		proxy.setMonitor(getMonitor());
		proxy.setDateFormatMap(genDateFormatMap());

		String sql = param.getValue(ParamKey.sql);
		logger.info(String.format("PostgreReader start to query %s .", sql));
		ResultSet rs = null;
        Connection connection = null;
		try {
            connection = getCon();
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            logger.info("set postgre statement fetch size");
            stmt.setFetchSize(fetchSize);
            logger.info("connection info autoCommit:" + connection.getAutoCommit() +
                " fetchSize" + stmt.getFetchSize());
			rs = DBUtils.query(stmt, sql);
            logger.info("get a ResultSet");
			proxy.sendToWriter(rs);
            logger.info("send to writer");
            proxy.flush();
            getMonitor().setStatus(PluginStatus.READ_OVER);
            return PluginStatus.SUCCESS.value();
		} catch (SQLException e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e);
		} finally {
            if (null != rs) {
                DBUtils.closeResultSet(rs);
            }
            if(connection != null) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }


	}

	@Override
	public int finish() {
		return PluginStatus.SUCCESS.value();
	}


    private boolean isSupportEncode(String encode) {
		if (supportEncode.contains(encode.toLowerCase())) {
			return true;
		}
		return false;
	}
    private  Connection getCon() {
        Connection connection = null;
        String url = "jdbc:postgresql://" + this.ip + ":" + this.port + "/" + this.dbname;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url,this.username,this.password);
        } catch (Exception e) {
            logger.error("get  connection error", e);
            throw new DataExchangeException(e);
        }
        return connection;
    }
	private Map<String, SimpleDateFormat> genDateFormatMap() {
		Map<String, SimpleDateFormat> mapDateFormat;
		mapDateFormat = new HashMap<String, SimpleDateFormat>();
		mapDateFormat.clear();
		mapDateFormat.put("date", new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss"));
		mapDateFormat.put("timestamp", new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss"));
		return mapDateFormat;
	}

	private Properties createProperties() {
		Properties p = new Properties();
//        "jdbc:postgresql://127.0.0.1:5432/test"
		String url = "jdbc:postgresql://" + this.ip + ":" + this.port + "/"
				+ this.dbname;

		p.setProperty("driverClassName", "org.postgresql.Driver");
		p.setProperty("url", url);
		p.setProperty("username", this.username);
		p.setProperty("password", this.password);
		p.setProperty("maxActive", String.valueOf(concurrency + 2));
		p.setProperty("initialSize", String.valueOf(concurrency + 2));
		p.setProperty("maxIdle", "1");
		p.setProperty("maxWait", "1000");
		p.setProperty("defaultReadOnly", "true");
		p.setProperty("testOnBorrow", "true");
		p.setProperty("validationQuery", "select 1");

		this.logger.info(String.format("PostgreReader try connection: %s .", url));
		return p;
	}
}
