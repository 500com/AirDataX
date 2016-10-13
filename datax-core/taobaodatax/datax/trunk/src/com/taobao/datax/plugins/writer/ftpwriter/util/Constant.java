package com.taobao.datax.plugins.writer.ftpwriter.util;


public class Constant {
	public static final int DEFAULT_FTP_PORT = 21;
	
	public static final int DEFAULT_SFTP_PORT = 22;
	
	public static final int DEFAULT_TIMEOUT = 60000;
	
	public static final int DEFAULT_MAX_TRAVERSAL_LEVEL = 100;
	
	public static final String  DEFAULT_FTP_CONNECT_PATTERN = "PASV";

	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final String DEFAULT_NULL_FORMAT = "\\N";

	public static final String FILE_FORMAT_CSV = "csv";

	public static final String FILE_FORMAT_TEXT = "text";
	public static final String DEFAULT_FILE_NAME = "data";

	//每个分块10MB，最大10000个分块
	public static final Long MAX_FILE_SIZE = 1024 * 1024 * 10 * 10000L;

	public static final String DEFAULT_SUFFIX = "";
}
