package com.taobao.datax.plugins.reader.ftpreader;

import com.google.common.collect.Lists;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.*;


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Created by zhuhq on 2016/9/20.
 */
public class FtpReader extends Reader {
    private static final Logger LOG = LoggerFactory.getLogger(FtpReader.class);
    // ftp链接参数
    private String protocol;
    private String host;
    private int port;
    private String username;
    private String password;
    private int timeout;
    private String connectPattern;
    private int maxTraversalLevel;
    private FtpHelper ftpHelper = null;

    private List<String> paths = null;
    private HashSet<String> sourceFiles;

    private String fileNameKey = "fileName";
    public static final String metaFileName = "__metadata";
    @Override
    public int init() {
        validateParameter();

        return PluginStatus.SUCCESS.value();
    }


    @Override
    public int prepare(PluginParam param) {


        LOG.debug("prepare() begin...");

        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int connect() {
        initFtpHelperAndLogin();
        return PluginStatus.SUCCESS.value();
    }
    private void initFtpHelperAndLogin() {
        if ("sftp".equals(protocol)) {
            //sftp协议
            this.ftpHelper = new SftpHelper();
        } else if ("ftp".equals(protocol)) {
            // ftp 协议
            this.ftpHelper = new StandardFtpHelper();
        }
        ftpHelper.loginFtpServer(host, username, password, port, timeout, connectPattern);
    }
    private void initMetadata(PluginParam param) {
        LOG.info("read metadata from file begin");
        String metaFileFullName = buildFilePath(paths.get(0), metaFileName, "");
        boolean metaFileIsExist = ftpHelper.isFileExist(metaFileFullName);
        LOG.info("meta file:" + metaFileFullName + "  exist:" + metaFileIsExist);
        if(metaFileIsExist){
            InputStream inputStream = null;
            try {
                inputStream = ftpHelper.getInputStream(metaFileFullName);
                ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
                MetaData metaData = (MetaData)objectInputStream.readObject();
                param.setMyMetaData(metaData);
                LOG.info("set my metadata finish");
            } catch (Exception e) {
                LOG.error("read metadata from file error: ", e);
            }finally {
                try {
                    inputStream.close();
                } catch (Exception ignore) {
                    LOG.warn("close inputStream error: ", ignore);
                }
            }
        }
        LOG.info("read metadata from file end");
    }
    private boolean isDataFile(String fileName) {
        return !fileName.endsWith(metaFileName);
    }
    @Override
    public List<PluginParam> split(PluginParam param) {
        initFtpHelperAndLogin();
        try {
            initMetadata(param);
        }catch (Exception ignore) {
            LOG.error("init metadata error", ignore);
        }
        List<PluginParam> pluginParams = new ArrayList<PluginParam>();
        System.out.println(StringUtils.join(paths,",") + "   "  + maxTraversalLevel);
        try {
            this.sourceFiles = ftpHelper.getAllFiles(paths, 0, maxTraversalLevel);
            LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
            for(String fileName : this.sourceFiles) {
                if(isDataFile(fileName)) {
                    PluginParam splitedParam = param.clone();
                    splitedParam.putValue(fileNameKey, fileName);
                    pluginParams.add(splitedParam);
                }
            }

        }finally {
            try {
                ftpHelper.logoutFtpServer();
            }catch (Exception ignore) {

            }

        }

        return pluginParams;
    }

    @Override
    public int startRead(LineSender sender) {
        LOG.debug("start read source files...");
        String encoding = param.getValue(ParamKey.ENCODING,
                Constant.DEFAULT_ENCODING);

        int bufferSize = param.getIntValue(ParamKey.BUFFER_SIZE,
                Constant.DEFAULT_BUFFER_SIZE);
        // warn: default value ',', fieldDelimiter could be \n(lineDelimiter)
        // for no fieldDelimiter
        Character fieldDelimiter = param.getCharValue(ParamKey.FIELD_DELIMITER,
                Constant.DEFAULT_FIELD_DELIMITER);
        Boolean skipHeader = param.getBoolValue(ParamKey.SKIP_HEADER,
                Constant.DEFAULT_SKIP_HEADER);

        String nullFormat = param.getValue(ParamKey.NULL_FORMAT, "\\N");
        String fileName = param.getValue(fileNameKey);
        if(StringUtils.isBlank(fileName)) {
            String message = "error file name: " + fileName;
            LOG.error(message);
            throw new DataExchangeException(message);
        }
        LOG.info(String.format("reading file : [%s]", fileName));
        InputStream inputStream = ftpHelper.getInputStream(fileName);
        int previous;
        String fetch;
        BufferedReader reader;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream,encoding), bufferSize);
            if(skipHeader) {
                String fetchLine = reader.readLine();
                LOG.info(String.format("Header line %s has been skiped.",
                        fetchLine));
            }
            while ((fetch = reader.readLine()) != null) {
                previous = 0;
                Line line = sender.createLine();
                for (int i = 0; i < fetch.length(); i++) {
                    if (fetch.charAt(i) == fieldDelimiter) {
                        line.addField(changeNull(nullFormat, fetch.substring(previous, i)));
                        previous = i + 1;
                    }
                }
                line.addField(fetch.substring(previous));
                sender.sendToWriter(line);
            }
            sender.flush();
        }  catch (Exception e) {
            LOG.error(ExceptionTracker.trace(e));
            throw new DataExchangeException(e.getCause());
        }


        LOG.debug("end read source files...");
        return PluginStatus.SUCCESS.value();
    }
    private static  String changeNull(String nullString, final String item) {
        if (nullString != null && nullString.equals(item)) {
            return null;
        }
        return item;
    }
    @Override
    public int finish() {
        try {
            this.ftpHelper.logoutFtpServer();
        } catch (Exception e) {
            String message = String.format(
                    "关闭与ftp服务器连接失败: [%s] host=%s, username=%s, port=%s",
                    e.getMessage(), host, username, port);
            LOG.error(message, e);
        }
        return PluginStatus.SUCCESS.value();
    }

    private void validateParameter() {
        this.port = param.getIntValue(ParamKey.PORT, Constant.DEFAULT_FTP_PORT);
        this.protocol = param.getValue(ParamKey.PROTOCOL, "sftp");
        boolean protocolTag = "ftp".equals(this.protocol) || "sftp".equals(this.protocol);
        if (!protocolTag) {
            throw new DataExchangeException(
                    String.format("仅支持 ftp和sftp 传输协议 , 不支持您配置的传输协议: [%s]", protocol));
        }
        this.host = param.getValue(ParamKey.HOST,"192.168.41.225");
        this.username = param.getValue(ParamKey.USERNAME,"");
        this.password = param.getValue(ParamKey.PASSWORD,"");

        this.timeout = param.getIntValue(ParamKey.TIMEOUT, Constant.DEFAULT_TIMEOUT);
        this.maxTraversalLevel = param.getIntValue(ParamKey.MAXTRAVERSALLEVEL, Constant.DEFAULT_MAX_TRAVERSAL_LEVEL);

        // only support connect pattern
        this.connectPattern = param.getValue(ParamKey.CONNECTPATTERN, Constant.DEFAULT_FTP_CONNECT_PATTERN);
        boolean connectPatternTag = "PORT".equals(connectPattern) || "PASV".equals(connectPattern);
        if (!connectPatternTag) {
            throw new DataExchangeException(
                    String.format("不支持您配置的ftp传输模式: [%s]", connectPattern));
        }

        //path check
        String pathInString = param.getValue(ParamKey.PATH,"");
        if("".equals(pathInString)) {
            throw new DataExchangeException("未指定path");
        }
        paths = Lists.newArrayList(pathInString.split(","));
        if (null == paths || paths.size() == 0) {
            throw new DataExchangeException("您需要指定待读取的源目录或文件");
        }
        for (String eachPath : paths) {
            if(!eachPath.startsWith("/")){
                String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
                LOG.error(message);
                throw new DataExchangeException(message);
            }
        }



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
