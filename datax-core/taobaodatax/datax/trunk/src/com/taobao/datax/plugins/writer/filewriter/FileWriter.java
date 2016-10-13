package com.taobao.datax.plugins.writer.filewriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.plugin.Line;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.MetaData;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.plugins.writer.ftpwriter.util.Constant;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.antcontrib.net.httpclient.Params;

/**
 * Created by zhuhq on 2016/9/22.
 */
public class FileWriter extends Writer{
    private final static Logger logger = LoggerFactory.getLogger(FileWriter.class);
    public static final String FILE_FORMAT_CSV = "csv";
    public static final String FILE_FORMAT_TEXT = "txt";
    private String fileFormat = FILE_FORMAT_CSV;
    private String path;
    private String fileName = "data";
    private char fieldSplit = '\t';
    private String writeMode = "truncate";
    private String suffix = "";
    private String encoding = "UTF-8";
    public static final String metaFileName = "__metadata";
    private Set<String> allFileExists = new HashSet<String>();


    @Override public int init() {
        this.path = param.getValue(ParamKey.path);
        if (!path.startsWith("/") && path.indexOf(":") != 1) {
            String message = String.format("请检查参数path:%s,需要配置为绝对路径", path);
            logger.error(message);
            throw new DataExchangeException(message);
        }
        if(this.path.indexOf("tmp") < 1) {
            String message = "path 路径中必须包含tmp  path:" + path;
            logger.error(message);
            throw new DataExchangeException(message);
        }
        this.fileFormat = param.getValue(ParamKey.fileFormat, FILE_FORMAT_CSV);
        this.fileName = param.getValue(ParamKey.fileName, fileName);
        this.fieldSplit = param.getCharValue(ParamKey.fieldSplit, fieldSplit);
        this.writeMode = param.getValue(ParamKey.writeMode, writeMode);
        this.suffix = param.getValue(ParamKey.suffix, suffix);
        this.encoding = param.getValue(ParamKey.encoding, encoding);
        return PluginStatus.SUCCESS.value();
    }

    @Override public int prepare(final PluginParam param) throws FileNotFoundException {
        File dir = new File(path);
        if ("truncate".equals(writeMode)) {
            logger.info(String.format(
                    "由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的内容",
                    path, fileName));

            // warn:需要判断文件是否存在，不存在时，不能删除
            try {
                if (dir.exists()) {
                    // warn:不要使用FileUtils.deleteQuietly(dir);
                    FilenameFilter filter = new PrefixFileFilter(fileName);
                    File[] filesWithFileNamePrefix = dir.listFiles(filter);
                    for (File eachFile : filesWithFileNamePrefix) {
                        logger.info(String.format("delete file [%s].",
                                eachFile.getName()));
                        FileUtils.forceDelete(eachFile);
                    }
                    // FileUtils.cleanDirectory(dir);
                }
            } catch (NullPointerException npe) {
                throw new DataExchangeException(
                                String.format("您配置的目录清空时出现空指针异常 : [%s]",
                                        path), npe);
            } catch (IllegalArgumentException iae) {
                throw new DataExchangeException(
                        String.format("您配置的目录参数异常 : [%s]", path));
            } catch (SecurityException se) {
                throw new DataExchangeException(
                        String.format("您没有权限查看目录 : [%s]", path));
            } catch (IOException e) {
                throw new DataExchangeException(
                        String.format("无法清空目录 : [%s]", path), e);
            }
        } else if ("append".equals(writeMode)) {
            logger.info(String
                    .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                            path, fileName));
        } else if ("nonConflict".equals(writeMode)) {
            logger.info(String.format(
                    "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
            // warn: check two times about exists, mkdirs
            try {
                if (dir.exists()) {
                    if (dir.isFile()) {
                        throw new DataExchangeException(
                                        String.format(
                                                "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                                path));
                    }
                    // fileName is not null
                    FilenameFilter filter = new PrefixFileFilter(fileName);
                    File[] filesWithFileNamePrefix = dir.listFiles(filter);
                    if (filesWithFileNamePrefix.length > 0) {
                        List<String> allFiles = new ArrayList<String>();
                        for (File eachFile : filesWithFileNamePrefix) {
                            allFiles.add(eachFile.getName());
                        }
                        logger.error(String.format("冲突文件列表为: [%s]",
                                StringUtils.join(allFiles, ",")));
                        throw new DataExchangeException(
                                String.format(
                                        "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                        path));
                    }
                } else {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw new DataExchangeException(
                                        String.format(
                                                "您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw new DataExchangeException(
                        String.format("您没有权限查看目录 : [%s]", path));
            }
        } else {
            throw new DataExchangeException(
                            String.format(
                                    "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                    writeMode));
        }
        if(!dir.exists()) {
            dir.mkdirs();
        }
        allFileExists.addAll(
                Arrays.asList(dir.list())
        );

        return PluginStatus.SUCCESS.value();
    }
    @Override
    public List<PluginParam> split(PluginParam param) {
        logger.info("begin do split...");
        Set<String> allFileExists = new HashSet<String>();
        allFileExists.addAll(this.allFileExists);
        List<PluginParam> pluginParams = new ArrayList<PluginParam>();
        int concurrency  = param.getIntValue(ParamKey.concurrency, 1);
        String filePrefix = this.fileName;
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
            splitedParam.putValue(ParamKey.fileName, fullFileName);
            logger.info(String
                    .format("splited write file name:[%s]", fullFileName));
            pluginParams.add(splitedParam);
        }
        logger.info("end do split.");
        return pluginParams;
    }
    @Override public int connect() {
        return PluginStatus.SUCCESS.value();
    }

    @Override public int post(final PluginParam param) {
        MetaData metaData = param.getOppositeMetaData();
        logger.info("my metadata: ", param.getMyMetaData());
        logger.info("meta data: " + metaData);
        if(metaData != null) {
            ObjectOutputStream objectOutputStream = null;
            try {
                String metaFileFullName = buildFilePath(path, metaFileName, "");
                objectOutputStream = new ObjectOutputStream(new FileOutputStream(metaFileFullName));
                objectOutputStream.writeObject(metaData);
                objectOutputStream.flush();
                logger.info("save metadata to file " + metaFileFullName);
            } catch (Exception e) {
                logger.error("writer metadata error: ", e);
            }finally {
                try {
                    objectOutputStream.close();
                } catch (Exception ignore) {
                    logger.warn("close objectOutputStream error: ", ignore);
                }
            }
        }
        return PluginStatus.SUCCESS.value();
    }

    @Override public int startWrite(final LineReceiver lineReceiver) {
        logger.info("begin do write...");
        String fileFullPath = buildFilePath(
                this.path, this.fileName, this.suffix);
        logger.info(String.format("write to file : [%s]", fileFullPath));

        OutputStream outputStream = null;
        try {

            outputStream = new FileOutputStream(fileFullPath);
            BufferedWriter bufferedWriter  = new BufferedWriter(new OutputStreamWriter(outputStream, encoding));

            FileWriterBack fileWriterBack = FileWriterBack.getWriter(fileFormat, fieldSplit,"",bufferedWriter);

            String headerStr = param.getValue(ParamKey.header,"");
            if (StringUtils.isNotBlank(headerStr)) {
                List<String> headers = Lists.newArrayList(headerStr.split(","));
                fileWriterBack.writeOneRecord(headers);
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
                    fileWriterBack.writeOneRecord(splitedRows);
                    //fileWriter.flush();
                } catch (Exception e) {
                    String message = String.format("write row error: %s ",StringUtils.join(splitedRows,fieldSplit));
                    logger.error(message, e);
                    throw new DataExchangeException(message, e);
                }
            }
            fileWriterBack.flush();
        } catch (Exception e) {
            throw new DataExchangeException(
                    String.format("无法创建待写文件 : [%s]", this.fileName), e);
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
        logger.info("end do write");
        return PluginStatus.SUCCESS.value();
    }

    @Override public int commit() {
        return PluginStatus.SUCCESS.value();
    }

    @Override public int finish() {
        return PluginStatus.SUCCESS.value();
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
