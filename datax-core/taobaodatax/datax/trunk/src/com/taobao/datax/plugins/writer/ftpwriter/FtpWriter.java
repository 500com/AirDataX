/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */


package com.taobao.datax.plugins.writer.ftpwriter;

import com.google.common.collect.Lists;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.*;

import com.taobao.datax.common.util.RetryUtil;
import com.taobao.datax.plugins.writer.ftpwriter.util.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.jasper.tagplugins.jstl.core.Param;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;


public class FtpWriter extends Writer {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FtpWriter.class);
	private Set<String> allFileExists = null;

	private String protocol;
	private String host;
	private int port;
	private String username;
	private String password;
	private int timeout;

	private String path;
	private String fileName;
	private String suffix;

	private IFtpHelper ftpHelper = null;
	
	@Override
	public int init() {
	    validateParameter();
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int connect() {
		initFtpHelperAndLogin();
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public List<PluginParam> split(PluginParam param) {
		LOG.info("begin do split...");
		Set<String> allFileExists = new HashSet<String>();
		allFileExists.addAll(this.allFileExists);
		List<PluginParam> pluginParams = new ArrayList<PluginParam>();
		String filePrefix = param.getValue(ParamKey.FILE_NAME, Constant.DEFAULT_FILE_NAME);
        int concurrency  = param.getIntValue(ParamKey.CONCURRENCY, 1);
		for (int i = 0; i < concurrency; i++) {
			// handle same file name
			PluginParam splitedParam = param.clone();
			String fileSuffix = System.nanoTime() + "";
			String fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
			//********防止文件名重复***********
			while (allFileExists.contains(fullFileName)) {
				fileSuffix = System.nanoTime() + "";
				fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
			}
			allFileExists.add(fullFileName);
			//================================
			splitedParam.putValue(ParamKey.FILE_NAME, fullFileName);
			LOG.info(String
					.format("splited write file name:[%s]", fullFileName));
			pluginParams.add(splitedParam);
		}
		LOG.info("end do split.");
		return pluginParams;
	}

	private void initFtpHelperAndLogin() {
		if ("sftp".equalsIgnoreCase(this.protocol)) {
			this.port = param.getIntValue(ParamKey.PORT,
					Constant.DEFAULT_SFTP_PORT);
			this.ftpHelper = new SftpHelperImpl();
		} else if ("ftp".equalsIgnoreCase(this.protocol)) {
			this.port = param.getIntValue(ParamKey.PORT,
					Constant.DEFAULT_FTP_PORT);
			this.ftpHelper = new StandardFtpHelperImpl();
		} else {
			throw new DataExchangeException(String.format(
					"仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]",
					protocol));
		}
		try {
			RetryUtil.executeWithRetry(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					ftpHelper.loginFtpServer(host, username, password,
							port, timeout);
					return null;
				}
			}, 3, 4000, true);
		} catch (Exception e) {
			String message = String
					.format("与ftp服务器建立连接失败, host:%s, username:%s, port:%s, errorMessage:%s",
							host, username, port, e.getMessage());
			LOG.error(message);
			throw new DataExchangeException(message, e);
		}
	}
	@Override
	public int startWrite(LineReceiver lineReceiver){
		LOG.info("begin do write...");
		String fileFullPath = buildFilePath(
				this.path, this.fileName, this.suffix);
		LOG.info(String.format("write to file : [%s]", fileFullPath));

		OutputStream outputStream = null;
		try {

			outputStream = this.ftpHelper.getOutputStream(fileFullPath);
			String encoding = param.getValue(ParamKey.ENCODING,
					Constant.DEFAULT_ENCODING);
			// handle blank encoding
			if (StringUtils.isBlank(encoding)) {
				LOG.warn(String.format("您配置的encoding为[%s], 使用默认值[%s]", encoding,
						Constant.DEFAULT_ENCODING));
				encoding = Constant.DEFAULT_ENCODING;
			}
			BufferedWriter bufferedWriter  = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));
		/*	String nullFormat = param.getValue(ParamKey.NULL_FORMAT);

			// 兼容format & dataFormat
			String dateFormat = param.getValue(ParamKey.DATE_FORMAT);
			DateFormat dateParse = null; // warn: 可能不兼容
			if (StringUtils.isNotBlank(dateFormat)) {
				dateParse = new SimpleDateFormat(dateFormat);
			}*/

			// warn: default false
			String fileFormat = param.getValue(ParamKey.FILE_FORMAT,
					Constant.FILE_FORMAT_CSV);

			String delimiterInStr = param.getValue(ParamKey.FIELD_DELIMITER, Constant.DEFAULT_FIELD_DELIMITER + "");
			if (null != delimiterInStr && 1 != delimiterInStr.length()) {
				throw new DataExchangeException(
						String.format("仅仅支持单字符切分, 您配置的切分为 : [%s]", delimiterInStr));
			}
			if (null == delimiterInStr) {
				LOG.warn(String.format("您没有配置列分隔符, 使用默认值[%s]",
						Constant.DEFAULT_FIELD_DELIMITER));
			}

			// warn: fieldDelimiter could not be '' for no fieldDelimiter
			char fieldDelimiter = param.getCharValue(ParamKey.FIELD_DELIMITER,
					Constant.DEFAULT_FIELD_DELIMITER);

			FileWriter fileWriter = FileWriter.getWriter(fileFormat, fieldDelimiter, bufferedWriter);

			String headerStr = param.getValue(ParamKey.HEADER,"");
			if (StringUtils.isNotBlank(headerStr)) {
				List<String> headers = Lists.newArrayList(headerStr.split(","));
				fileWriter.writeOneRecord(headers);
			}

			Line line = null;
			while ((line = lineReceiver.getFromReader()) != null) {
				List<String> splitedRows = new ArrayList<String>();
				try {
					int fieldNum = line.getFieldNum();
					if (0 != fieldNum) {
						for (int i = 0; i < fieldNum; i++) {
							splitedRows.add(i, line.getField(i));
						}
					}
					fileWriter.writeOneRecord(splitedRows);
					//fileWriter.flush();
				} catch (Exception e) {
					String message = String.format("write row error: %s ",StringUtils.join(splitedRows,fieldDelimiter));
					LOG.error(message, e);
					throw new DataExchangeException(message, e);
				}
			}
            fileWriter.flush();
		} catch (Exception e) {
			throw new DataExchangeException(
					String.format("无法创建待写文件 : [%s]", this.fileName), e);
		} finally {
			IOUtils.closeQuietly(outputStream);
		}
		LOG.info("end do write");
		return PluginStatus.SUCCESS.value();

	}

	@Override
	public int commit() {
		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int prepare(PluginParam param) {
		initFtpHelperAndLogin();
		try {
			String path = param.getValue(ParamKey.PATH);
			// warn: 这里用户需要配一个目录
			this.ftpHelper.mkdir(path);

			String fileName = param
					.getValue(ParamKey.FILE_NAME, Constant.DEFAULT_FILE_NAME);
			String writeMode = param
					.getValue(ParamKey.WRITE_MODE);

			Set<String> allFileExists = this.ftpHelper.getAllFilesInDir(path,
					fileName);
			this.allFileExists = allFileExists;

			// truncate option handler
			if ("truncate".equals(writeMode)) {
				LOG.info(String.format(
						"由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
						path, fileName));
				Set<String> fullFileNameToDelete = new HashSet<String>();
				for (String each : allFileExists) {
					fullFileNameToDelete.add(buildFilePath(path, each, null));
				}
				LOG.info(String.format(
						"删除目录path:[%s] 下指定前缀fileName:[%s] 文件列表如下: [%s]", path,
						fileName,
						StringUtils.join(fullFileNameToDelete.iterator(), ", ")));

				this.ftpHelper.deleteFiles(fullFileNameToDelete);
			} else if ("append".equals(writeMode)) {
				LOG.info(String
						.format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
								path, fileName));
				LOG.info(String.format(
						"目录path:[%s] 下已经存在的指定前缀fileName:[%s] 文件列表如下: [%s]",
						path, fileName,
						StringUtils.join(allFileExists.iterator(), ", ")));
			} else if ("nonConflict".equals(writeMode)) {
				LOG.info(String.format(
						"由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
				if (!allFileExists.isEmpty()) {
					LOG.info(String.format(
							"目录path:[%s] 下指定前缀fileName:[%s] 冲突文件列表如下: [%s]",
							path, fileName,
							StringUtils.join(allFileExists.iterator(), ", ")));
					throw new DataExchangeException(
							String.format(
									"您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
									path));
				}
			} else {
				throw new DataExchangeException(
						String.format(
								"仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
								writeMode));
			}
		} finally {
			try {
				ftpHelper.logoutFtpServer();
			}catch (Exception ignore) {
				LOG.warn("logout error",ignore);
			}
		}


		return PluginStatus.SUCCESS.value();
	}

	@Override
	public int finish() {
		try {
			ftpHelper.logoutFtpServer();
		}catch (Exception ignore) {
			LOG.warn("logout error",ignore);
		}

		return PluginStatus.SUCCESS.value();
	}


	private void validateParameter() {
		this.path = param.getValue(ParamKey.PATH, "");
		if (!path.startsWith("/") && path.indexOf(":") != 1) {
			String message = String.format("请检查参数path:%s,需要配置为绝对路径", path);
			LOG.error(message);
			throw new DataExchangeException(message);
		}
        if(this.path.indexOf("tmp") < 1) {
            String message = "path 路径中必须包含tmp";
            LOG.error(message);
            throw new DataExchangeException(message);

        }
        this.fileName = param.getValue(ParamKey.FILE_NAME, Constant.DEFAULT_FILE_NAME);
		this.suffix = param.getValue(ParamKey.SUFFIX, Constant.DEFAULT_SUFFIX);
		this.host = param.getValue(ParamKey.HOST, "192.168.41.225");
		this.username = param.getValue(ParamKey.USERNAME,"");
		this.password = param.getValue(ParamKey.PASSWORD, "");
		this.timeout = param.getIntValue(ParamKey.TIMEOUT, Constant.DEFAULT_TIMEOUT);

		this.protocol = param.getValue(
				ParamKey.PROTOCOL, "sftp");


	}

	private static String buildFilePath(String path, String fileName,
									   String suffix) {
        char lastChar = path.charAt(path.length() - 1);
        if(lastChar != '/' && lastChar != '\\') {
            path = path + IOUtils.DIR_SEPARATOR;
        }
		if (null == suffix) {
			suffix = "";
		} else {
			suffix = suffix.trim();
		}
		return String.format("%s%s%s", path, fileName, suffix);
	}

}
