/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */

package com.taobao.datax.plugins.writer.oraclewriter;

import com.taobao.datax.common.constants.Constants;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.plugin.*;
import com.taobao.datax.common.util.EnvUtils;
import com.taobao.datax.common.util.StrUtils;
import org.apache.log4j.Logger;

import java.util.List;

public class OracleWriter extends Writer {

	/* \001 is field split, \002 is line split */
	private static final char[] replaceChars = { '\001', 0, '\002', 0 };
	private Logger logger = Logger.getLogger(OracleWriter.class);

	private String password;

	private String username;

	private String dbname;

	private String table;

	private static final char SEP = '\001';

	private static final char BREAK = '\002';

	private String pre;

	private String post;

	private String dtfmt;

	private String encoding;

	private String colorder;

	private String limit;

	private long concurrency;

	private String logon;

	private long p;

	private int save2server;

	private int commit2server;

	private long skipindex;
    StringBuilder sb = new StringBuilder(10240000);
	static {
		String writerSharedObjectPath = Constants.DATAX_LOCATION
				+ "/plugins/writer/oraclewriter/";
		System.setProperty("java.library.path",
				System.getProperty("java.library.path") + ":"
						+ writerSharedObjectPath);
		EnvUtils.putEnv("LD_LIBRARY_PATH", writerSharedObjectPath + ":"
				+ EnvUtils.getEnv("LD_LIBRARY_PATH"));
	}

	@Override
	public List<PluginParam> split(PluginParam param) {
		OracleWriterSplitter spliter = new OracleWriterSplitter();
		spliter.setParam(param);
		spliter.init();
		return spliter.split();
	}

	@Override
	public int prepare(PluginParam param) {
        int resultCode = -1;
		try {
			if (!pre.isEmpty()) {
				this.logger.info(String
						.format("OracleWriter starts to execute pre-sql %s .",
								this.pre));
				p = OracleWriterJni.getInstance().oracle_dumper_init(logon,
						table, String.valueOf(SEP), pre, post, dtfmt, encoding,
						colorder, limit, concurrency, skipindex);
				OracleWriterJni.getInstance().oracle_dumper_connect(p);
				OracleWriterJni.getInstance().oracle_dumper_predump(p, 0);
                resultCode = OracleWriterJni.getInstance().oracle_dumper_finish(p, 1);
			}
		} catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e.getCause());
		}

		return resultCode == 0 ? PluginStatus.SUCCESS.value():PluginStatus.FAILURE.value();
	}

	@Override
	public int post(PluginParam param) {
        int resultCode = -1;
		try {
			if (!post.isEmpty()) {
				this.logger.info(String.format(
						"OracleWriter starts to execute post-sql %s .",
						this.post));
				p = OracleWriterJni.getInstance().oracle_dumper_init(logon,
						table, String.valueOf(SEP), pre, post, dtfmt, encoding,
						colorder, limit, concurrency, skipindex);
				OracleWriterJni.getInstance().oracle_dumper_connect(p);
                resultCode = OracleWriterJni.getInstance().oracle_dumper_finish(p, 3); // end
			}

		} catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e.getCause());
		}
        return resultCode == 0 ? PluginStatus.SUCCESS.value():PluginStatus.FAILURE.value();
	}

	@Override
	public int init() {
		password = param.getValue(ParamKey.password, "");
		username = param.getValue(ParamKey.username, "");

		dbname = param.getValue(ParamKey.dbname, "");
		table = param.getValue("schema") + "."
				+ param.getValue(ParamKey.table, "");
		dtfmt = StrUtils.removeSpace(param.getValue(ParamKey.dtfmt, ""), ",");
		pre = param.getValue(ParamKey.pre, "");
		post = param.getValue(ParamKey.post, "");
		encoding = param.getValue(ParamKey.encoding, "UTF-8");
		colorder = StrUtils.removeSpace(param.getValue(ParamKey.colorder, ""),
				",");
		limit = param.getValue(ParamKey.limit, "");
		concurrency = param.getIntValue(ParamKey.concurrency, 1);

		commit2server = 50000;
		save2server = 1000;
		skipindex = 0;
		logon = username + "/" + password + "@" + dbname;

		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int connect() {
        int resultCode = -1;
		try {
			p = OracleWriterJni.getInstance().oracle_dumper_init(logon, table,
					String.valueOf(SEP), "", post, dtfmt, encoding, colorder,
					limit, concurrency, skipindex);
			resultCode = OracleWriterJni.getInstance().oracle_dumper_connect(p);
		} catch (Exception e) {
			logger.error("connect error",e);
			throw new DataExchangeException(e);
		}
		return resultCode == 0 ? PluginStatus.SUCCESS.value():PluginStatus.FAILURE.value();
	}

	@Override
	public int startWrite(LineReceiver resultHandler) {
        int resultCode = -1;
		try {
            resultCode = OracleWriterJni.getInstance().oracle_dumper_predump(p, 1);
            checkCppEngineResultCode(resultCode);
			Line line = null;
			String field;
			int iCount = 0;
			int iCount1 = 0;

			//StringBuilder sb = new StringBuilder(1024000);
			while ((line = resultHandler.getFromReader()) != null) {
				int num = line.getFieldNum();
				for (int i = 0; i < num; i++) {
					field = line.getField(i);
					if (null != field) {
						sb.append(StrUtils.replaceChars(field, replaceChars));
					} /*
					 * else { sb.append(""); }
					 */
					sb.append(SEP);
				}
				sb.delete(sb.length() - 1, sb.length());
				sb.append(BREAK);

				if (iCount == save2server) {
                    resultCode = OracleWriterJni.getInstance().oracle_dumper_dump(p,
							sb.toString());
                    checkCppEngineResultCode(resultCode);

					sb.setLength(0);
					iCount = 0;
				}
				iCount++;

				if (iCount1 == commit2server) {
					sb.append(BREAK + "1y9i8x7i0a3o2*5" + BREAK);
                    resultCode = OracleWriterJni.getInstance().oracle_dumper_dump(p,
							sb.toString());
                    checkCppEngineResultCode(resultCode);

					sb.setLength(0);
					commit();
					iCount1 = 0;
				}
				iCount1++;
			}
			sb.append(BREAK + "1y9i8x7i0a3o2*5" + BREAK);

            resultCode = OracleWriterJni.getInstance().oracle_dumper_dump(p, sb.toString());
		} catch (Exception e) {
			logger.error("error",e);
			throw new DataExchangeException(e);
		}
        return  resultCode == 0 ? PluginStatus.SUCCESS.value():PluginStatus.FAILURE.value();
	}

	@Override
	public int commit() {
		try {
			int ret = OracleWriterJni.getInstance().oracle_dumper_commit(p);
			if (0 != ret) {
				return PluginStatus.FAILURE.value();
			}
			return PluginStatus.SUCCESS.value();
		} catch (Exception e) {
			logger.error("commit error",e);
			throw new DataExchangeException(e);
		}
	}

	@Override
	public int finish() {
        int resultCode = PluginStatus.FAILURE.value();
		try {
			int discard = OracleWriterJni.getInstance().oracle_dumper_finish(p,
					1);
            if(discard > 0) {
                throw new DataExchangeException("oracle writer discard data row:" + discard);
            }else if (discard < 0) {
                throw new DataExchangeException("oracle writer c++ engine error,code:" + discard);
            }else {
                resultCode =  PluginStatus.SUCCESS.value();
            }
			this.getMonitor().setFailedLines(discard);

		} catch (Exception e) {
			logger.error("finish error",e);
			throw new DataExchangeException(e);
		}
        return  resultCode;
	}

    private void checkCppEngineResultCode(int code) {
        if(code != 0) {
            throw new DataExchangeException("oracle writer c++ engine give an error code:" + code);
        }
    }
}
