/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 */

package com.taobao.datax.plugins.writer.hdfswriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.net.URI;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.taobao.datax.common.plugin.*;
import com.taobao.datax.common.util.ArrayUtils;
import com.taobao.datax.plugins.common.DBUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.plugins.common.DFSUtils;

import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

public class HdfsWriter extends Writer {

    private static final Logger logger = Logger.getLogger(HdfsWriter.class);

    private FileSystem fs;

    private Path p = null;

    private char FIELD_SPLIT = '\u0001';

    private char LINE_SPLIT = '\n';

    private int BUFFER_SIZE = 8 * 1024;

    private String ENCODING = "UTF-8";

    private String delMode = "3";

    private String hadoop_conf = "";
    private Configuration configuration;
    private int concurrency = 10;
    private char[] nullChars = null;
    private boolean isSwith2HiveTable = false;
    private static char[] searchChars = new char[2];

    private DfsWriterStrategy dfsWriterStrategy = null;


    static {
        Thread.currentThread().setContextClassLoader(HdfsWriter.class.getClassLoader());
    }

	/*
     * NOTE: if user set parameter 'splitnum' to 1, which means no-split in
	 * hdfswriter, we use dir + prefixname as the fixed hdfs-file name for
	 * example: dir = hdfs://taobao/dw prefixname = bazhen.csy splitname = 1 we
	 * use hdfs://taobao/dw/bazhen.csy as the target filename which hdfswriter
	 * dump file to
	 * 
	 * for other cases, we use prefixname as just prefix filename, for example
	 * dir = hdfs://taobao/dw prefixname = bazhen.csy splitname = 2 at last, the
	 * generated filename will be hdfs://taobao/dw/bazhen.csy-0
	 * hdfs://taobao/dw/bazhen.csy-1 the suffix is thread number
	 */

    @Override
    public int prepare(PluginParam param) {
        String dir = param.getValue(ParamKey.dir);
        String ugi = param.getValue(ParamKey.ugi, null);
        String prefixname = param.getValue(ParamKey.prefixname,
                "prefix");
        delMode = param.getValue(ParamKey.delMode, this.delMode);
        concurrency = param.getIntValue(ParamKey.concurrency, 1);
        hadoop_conf = param.getValue(ParamKey.hadoop_conf, "");

        if (dir.endsWith("*")) {
            dir = dir.substring(0, dir.lastIndexOf("*"));
        }
        if (dir.endsWith("/")) {
            dir = dir.substring(0, dir.lastIndexOf("/"));
        }

        Path rootpath = new Path(dir);
        try {
            configuration = DFSUtils.getConf(dir, ugi, hadoop_conf);
            fs = DFSUtils.createFileSystem(new URI(dir),
                    configuration);

			/* No split to dump file, use dir as absolute filename . */
            if (concurrency == 1) {
                DFSUtils.deleteFile(fs, new Path(dir + "/" + prefixname), true);
            }
			/* use dir as directory path . */
            else {
                if ("4".equals(delMode))
                    DFSUtils.deleteFiles(fs, rootpath, true, true);
                else if ("3".equals(delMode))
                    DFSUtils.deleteFiles(fs, new Path(dir + "/" + prefixname
                            + "-*"), true, true);
            }
            if(isSwith2HiveTable) {
                switch2HiveTablePre();
            }
        } catch (Exception e) {
            logger.error(ExceptionTracker.trace(e));
            throw new DataExchangeException(String.format(
                    "HdfsWriter Init file system failed:%s,%s", e.getMessage(),
                    e.getCause()));
        } finally {
            closeAll();
        }

        return PluginStatus.SUCCESS.value();
    }

    @Override
    public List<PluginParam> split(PluginParam param) {
        HdfsFileSplitter spliter = new HdfsFileSplitter();
        spliter.setParam(param);
        spliter.init();
        return spliter.split();
    }

    @Override
    public int init() {
        FIELD_SPLIT = param.getCharValue(ParamKey.fieldSplit,
                FIELD_SPLIT);
        ENCODING = param.getValue(ParamKey.encoding, ENCODING);
        LINE_SPLIT = param.getCharValue(ParamKey.lineSplit,
                LINE_SPLIT);
        searchChars[0] = FIELD_SPLIT;
        searchChars[1] = LINE_SPLIT;
        BUFFER_SIZE = param.getIntValue(ParamKey.bufferSize,
                BUFFER_SIZE);
        delMode = param.getValue(ParamKey.delMode, this.delMode);
        nullChars = param.getValue(ParamKey.nullChar, "")
                .toCharArray();
        hadoop_conf = param.getValue(ParamKey.hadoop_conf, "");

        String ugi = param.getValue(ParamKey.ugi, null);
        String dir = param.getValue(ParamKey.dir);
        isSwith2HiveTable = "true".equalsIgnoreCase(param.getValue(ParamKey.hiveTableSwitch, "false"));
        try {
            configuration = DFSUtils.getConf(dir, ugi, hadoop_conf);
            fs = DFSUtils.createFileSystem(new URI(dir),
                    configuration);
        } catch (Exception e) {
            logger.error(ExceptionTracker.trace(e));
            closeAll();
            throw new DataExchangeException(String.format(
                    "HdfsWriter Initialize file system failed:%s,%s",
                    e.getMessage(), e.getCause()));
        }

        if (dir != null) {
            p = new Path(dir);
        } else {
            closeAll();
            throw new DataExchangeException("Can't find the param ["
                    + ParamKey.dir + "] in hdfs-writer-param.");
        }

        String fileType = param.getValue(ParamKey.fileType, "TXT");
        if ("SEQ".equalsIgnoreCase(fileType)
                || "SEQ_COMP".equalsIgnoreCase(fileType))
            dfsWriterStrategy = new DfsWriterSequeueFileStrategy();
        else if ("TXT_COMP".equalsIgnoreCase(fileType))
            dfsWriterStrategy = new DfsWriterTextFileStrategy(true);
        else if ("TXT".equalsIgnoreCase(fileType))
            dfsWriterStrategy = new DfsWriterTextFileStrategy(false);
        else if ("PARQUET".equalsIgnoreCase(fileType))
            dfsWriterStrategy = new DfsWriterParquetFileStrategy();
        else {
            closeAll();
            throw new DataExchangeException(
                    "HdfsWriter cannot recognize filetype: " + fileType);
        }

        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int connect() {
        if (p == null) {
            closeAll();
            throw new DataExchangeException(
                    "HdfsWriter Can't initialize file system .");
        }
        try {
            if ("2".equals(delMode))
                DFSUtils.deleteFile(fs, p, true);

            dfsWriterStrategy.open();

            getMonitor().setStatus(PluginStatus.CONNECT);

            return PluginStatus.SUCCESS.value();
        } catch (Exception ex) {
            closeAll();
            logger.error(ExceptionTracker.trace(ex));
            throw new DataExchangeException(String.format(
                    "HdfsWriter initialize file system failed: %s, %s",
                    ex.getMessage(), ex.getCause()));
        }
    }

    @Override
    public int startWrite(LineReceiver receiver) {
        getMonitor().setStatus(PluginStatus.WRITE);

        try {
            dfsWriterStrategy.write(receiver);
        } catch (Exception ex) {
            throw new DataExchangeException(String.format(
                    "Some errors occurs on starting writing: %s,%s",
                    ex.getMessage(), ex.getCause()));
        } finally {
            dfsWriterStrategy.close();
            closeAll();
        }
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int commit() {
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int finish() {
        closeAll();
        getMonitor().setStatus(PluginStatus.WRITE_OVER);
        return PluginStatus.SUCCESS.value();
    }

    private void closeAll() {
        try {
            IOUtils.closeStream(fs);
        } catch (Exception e) {
            throw new DataExchangeException(String.format(
                    "HdfsWriter closing filesystem failed: %s,%s",
                    e.getMessage(), e.getCause()));
        }
    }

    @Override
    public int cleanup() {
        closeAll();
        return PluginStatus.SUCCESS.value();
    }

    public interface DfsWriterStrategy {
        void open();

        void write(LineReceiver receiver);

        void close();
    }

    class DfsWriterSequeueFileStrategy implements DfsWriterStrategy {
        private Configuration conf = null;

        private SequenceFile.Writer writer = null;

        private Writable key = null;

        private Writable value = null;

        private boolean compressed = false;

        private String keyClassName = null;

        private String valueClassName = null;

        private Class<?> keyClass = null;

        private Class<?> valueClass = null;

        private Method keySetMethod = null;

        private Method valueSetMethod = null;

        // modified by bazhen.csy
        private int keyFieldIndex = -1;

        private SequenceFile.CompressionType compressioType = SequenceFile.CompressionType.BLOCK;

        public DfsWriterSequeueFileStrategy() {
            super();
            String cType = getParam().getValue(
                    ParamKey.compressionType, "NONE");
            if ("BLOCK".equalsIgnoreCase(cType)) {
                compressioType = SequenceFile.CompressionType.BLOCK;
                compressed = true;
            } else if ("RECORD".equalsIgnoreCase(cType)) {
                compressioType = SequenceFile.CompressionType.RECORD;
                compressed = true;
            } else {
                compressioType = SequenceFile.CompressionType.NONE;
            }
            this.conf = DFSUtils.newConf();
        }

        @Override
        public void close() {
            IOUtils.closeStream(writer);
        }

        @Override
        public void write(LineReceiver resultHandler) {
            Line line = null;
            try {

                StringBuilder sb = new StringBuilder(10240);
                while ((line = resultHandler.getFromReader()) != null) {
                    int len = line.getFieldNum();
                    for (int i = 0; i < len; i++) {
                        if (i == keyFieldIndex) {
                            if (keySetMethod != null)
                                keySetMethod
                                        .invoke(key,
                                                new Object[]{adapterType(
                                                        line.getField(i),
                                                        keyClassName)});

                        } else {
                            sb.append(
                                    replaceChars(line.getField(i), searchChars))
                                    .append(FIELD_SPLIT);
                        }
                    }
                    sb.delete(sb.length() - 1, sb.length());
                    if (valueSetMethod != null)
                        valueSetMethod.invoke(
                                value,
                                new Object[]{adapterType(sb.toString(),
                                        valueClassName)});
                    writer.append(key, value);
                    sb.setLength(0);
                }
            } catch (Exception e) {
                logger.error(ExceptionTracker.trace(e));
                throw new DataExchangeException(e);
            }
        }

        @Override
        public void open() {
            try {
                if ("1".equals(delMode) && fs.exists(p))
                    throw new DataExchangeException("the file [" + p.getName()
                            + "] already exists. ");

                String codecClassName = getParam().getValue(
                        ParamKey.codecClass,
                        "org.apache.hadoop.io.compress.DefaultCodec");
                keyClassName = getParam().getValue(
                        ParamKey.keyClass,
                        "org.apache.hadoop.io.Text");
                valueClassName = getParam().getValue(
                        ParamKey.valueClass,
                        "org.apache.hadoop.io.Text");

                keyClass = Class.forName(keyClassName);
                valueClass = Class.forName(valueClassName);

                key = (Writable) ReflectionUtils.newInstance(keyClass, conf);
                value = (Writable) ReflectionUtils
                        .newInstance(valueClass, conf);

                keyFieldIndex = param.getIntValue(
                        ParamKey.keyFieldIndex, this.keyFieldIndex);

                if (!keyClassName.toLowerCase().contains("null")
                        && (keyFieldIndex >= 0))
                    keySetMethod = keyClass.getMethod(
                            "set",
                            new Class[]{DFSUtils.getTypeMap().get(
                                    keyClassName)});
                if (!valueClassName.toLowerCase().contains("null"))
                    valueSetMethod = valueClass.getMethod(
                            "set",
                            new Class[]{DFSUtils.getTypeMap().get(
                                    valueClassName)});

                if (compressed) {
                    Class<?> codecClass = Class.forName(codecClassName);
                    CompressionCodec codec = (CompressionCodec) ReflectionUtils
                            .newInstance(codecClass, conf);
                    writer = SequenceFile.createWriter(fs, conf, p, keyClass,
                            valueClass, compressioType, codec);
                } else
                    writer = SequenceFile.createWriter(fs, conf, p, keyClass,
                            valueClass);

            } catch (Exception e) {
                throw new DataExchangeException(e);
            }
        }

        private Object adapterType(String field, String typename) {
            Object target = null;
            if (typename.toLowerCase().contains("null")) {
                target = null;
            } else if (typename.toLowerCase().contains("text")) {
                target = field;
            } else if (typename.toLowerCase().contains("long")) {
                target = Long.parseLong(field);
            } else if (typename.toLowerCase().contains("integer")) {
                target = Integer.parseInt(field);
            } else if (typename.toLowerCase().contains("double")) {
                target = Double.parseDouble(field);
            } else if (typename.toLowerCase().contains("float")) {
                target = Float.parseFloat(field);
            } else {
                target = field;
            }
            return target;
        }
    }

    class DfsWriterTextFileStrategy implements DfsWriterStrategy {
        private FSDataOutputStream out = null;

        private BufferedWriter bw = null;

        private CompressionOutputStream co = null;

        private boolean compressed = false;

        public DfsWriterTextFileStrategy(boolean compressed) {
            super();
            this.compressed = compressed;
        }

        @Override
        public void close() {
            IOUtils.cleanup(null, bw, out, co);
        }

        @Override
        public void open() {
            try {
                boolean flag = false;
                if ("0".equals(delMode))
                    flag = true;
                if (compressed) {
                    String codecClassName = param.getValue(
                            ParamKey.codecClass,
                            "org.apache.hadoop.io.compress.DefaultCodec");

                    Class<?> codecClass = Class.forName(codecClassName);
                    Configuration conf = DFSUtils.newConf();
                    CompressionCodec codec = (CompressionCodec) ReflectionUtils
                            .newInstance(codecClass, conf);

                    out = fs.create(p, flag, BUFFER_SIZE);
                    co = codec.createOutputStream(out);
                    bw = new BufferedWriter(
                            new OutputStreamWriter(co, ENCODING), BUFFER_SIZE);
                } else {
                    out = fs.create(p, flag, BUFFER_SIZE);
                    bw = new BufferedWriter(new OutputStreamWriter(out,
                            ENCODING), BUFFER_SIZE);
                }
            } catch (Exception e) {
                logger.error(ExceptionTracker.trace(e));
                throw new DataExchangeException(e);
            }
        }

        @Override
        public void write(LineReceiver receiver) {
            Line line;
            try {
                while ((line = receiver.getFromReader()) != null) {
                    int len = line.getFieldNum();
                    for (int i = 0; i < len; i++) {
                        // bw.write(line.getField(i));
                        bw.write(replaceChars(line.getField(i), searchChars));
                        if (i < len - 1)
                            bw.write(FIELD_SPLIT);
                    }
                    bw.write(LINE_SPLIT);
                }
                bw.flush();
            } catch (Exception e) {
                logger.error(ExceptionTracker.trace(e));
                throw new DataExchangeException(e);
            }
        }

    }
    class DfsWriterParquetFileStrategy implements DfsWriterStrategy {
        private ParquetWriter<Group> writer;
        public DfsWriterParquetFileStrategy() {

        }
        private  ParquetWriter<Group> initWriter(Path filePath,Configuration conf,MessageType schema)
                throws IOException {


            GroupWriteSupport.setSchema(schema, conf);


            ParquetWriter<Group> writer = new ParquetWriter<Group>(
                    filePath,
                    new GroupWriteSupport(),
                    CompressionCodecName.UNCOMPRESSED,
                    1024,
                    1024,
                    512,
                    true,
                    false,
                   // ParquetProperties.WriterVersion.PARQUET_1_0,
                    conf);

            return writer;
        }
        @Override
        public void close() {
            if(writer != null) {
                IOUtils.closeStream(writer);
            }
        }

        @Override
        public void open() {
            try {
                /*boolean flag = false;
                if ("0".equals(delMode))
                    flag = true;*/

            } catch (Exception e) {
                logger.error(ExceptionTracker.trace(e));
                throw new DataExchangeException(e);
            }
        }

        @Override
        public void write(LineReceiver receiver) {
            Line line;
            MessageType schema = null;
            SimpleGroupFactory sfg = null;
            MetaData metaData = null;
            try {
                while ((line = receiver.getFromReader()) != null) {
                    int len = line.getFieldNum();

                    if(writer == null) {
                        logger.info("parquet writer init begin");
                        metaData = HdfsWriterUtil.getHiveTableMetaData(param);
                        schema = initSchema(param,metaData);
                        GroupWriteSupport.setSchema(schema, configuration);
                        sfg = new SimpleGroupFactory(schema);
                        writer = initWriter(p,configuration,schema);
                        logger.info("parquet writer init end");
                    }
                    List<MetaData.Column> columns = metaData.getColInfo();
                    int schemaFieldCount = columns.size();
                    len = Math.min(len,schemaFieldCount);
                    Group group = sfg.newGroup();
                    for (int i = 0; i < len; i++) {
                        String value = String.valueOf(replaceChars(line.getField(i), searchChars));
                        MetaData.Column col = columns.get(i);
                        append2Group(group, schema.getFieldName(i), value, col.getDataType());
                    }
                    writer.write(group);

                }
            } catch (Exception e) {
                logger.error(ExceptionTracker.trace(e));
                throw new DataExchangeException(e);
            }
        }
        private MessageType initSchema(PluginParam param,MetaData metaData) {
            return HdfsWriterUtil.createParquetSchema(param,metaData);
        }
        private void append2Group(Group group,String name,String value,String columnType) {
            String parquetType = HdfsWriterUtil.hiveFieldType2ParquetType(columnType);
            boolean isNull = value == null || "".equals(value);
            if(isNull) {
                return;
            }
            try {
                if("binary".equals(parquetType)) {
                    group.append(name,value);
                }else if("int32".equals(parquetType)) {
                    group.append(name, Integer.valueOf(value));
                }else if("int64".equals(parquetType)) {
                    group.append(name, Long.valueOf(parquetType));
                }else if("float".equals(parquetType)) {
                    group.append(name, Float.valueOf(value));
                }else if("double".equals(parquetType)) {
                    group.append(name, Double.valueOf(value));
                }else {
                    logger.info("cast unsupported parquetType:" + parquetType);
                }
            }catch (Exception ex) {
                logger.error("cast value error:",ex);
                group.append(name,value);
            }
        }

    }
    private char[] replaceChars(String str, char[] searchChars) {
        if (null == str) {
            return this.nullChars;
        }
        char[] newchars = str.toCharArray();
        int strLength = str.length();
        for (int i = 0; i < strLength; i++) {
            if (searchChars[0] == newchars[i] || 13 == newchars[i]
                    || 10 == newchars[i]) {
                newchars[i] = ' ';
            }
        }
        return newchars;
    }

    // TODO : 自动建立hive表和分区
    @Override
    public int post(PluginParam param) {

        if (!isSwith2HiveTable)
            return 0;
/*
        MetaData md = param.getOppositeMetaData();
        // Vector<Column> cloInfo = md.getColInfo();
        if (md == null) {
            logger.error("md is null");
            throw new DataExchangeException("md is null");
        }*/

        String tableName = getTableName(param);
        String fileType = getHiveStorageFileType(param);
        String dataLocal = param.getValue(ParamKey.dir, null); //hive将要加载的文本所在的位置

        logger.info("tableName: " + tableName);
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            //switch2HiveTablePre();
            // 连接hive server
            con = getHiveConnection(param);
            /*SessionState ss = new SessionState(new HiveConf(SessionState.class));
            SessionState.start(ss);*/
            stmt = con.createStatement();

            boolean tableExsit = tableExist(stmt, tableName);

            String origParNameInfo = param.getValue(
                    ParamKey.partitionNames, "").trim();
            String origParValueInfo = param.getValue(
                    ParamKey.partitionValues, "").trim();
            logger.info("orgiParNameInfo:" + origParNameInfo);
            logger.info("orgiParValueInfo:" + origParValueInfo);
            String[] partitionNames = null;
            String[]  partitionValues = null;
            boolean isPartitionTable = false;
            if(StringUtils.isNotEmpty(origParNameInfo) && StringUtils.isNotEmpty(origParValueInfo)) {

                partitionNames = StringUtils.split(origParNameInfo,',');
                partitionValues = StringUtils.split(origParValueInfo,',');
                if(partitionNames == null || partitionNames.length != partitionValues.length) {
                    logger.error("ERROR: the number of partition name not equals to partition value");
                    throw new DataExchangeException();
                }
                isPartitionTable = true;
            }
            /*
            if (tableExsit)
                partitionNames = getPartitionNames(stmt, tableName);*/

            //setPartitionInfo(partitionNames, partitionValues, param, dataLocal);
            String partitionName = "";
            String partitionInfo = "";
            if(isPartitionTable) {
                int partitionNameSize = partitionNames.length;
                for (int i = 0; i < partitionNameSize; i++) {
                    if(i != 0) {
                        partitionName += ",";
                        partitionInfo += ",";
                    }
                    partitionName += partitionNames[i] + " STRING";
                    partitionInfo += partitionNames[i] + "='"
                            + partitionValues[i] + "'";
                }
            }
            if (!tableExsit) {


                String columnInfo = getColumnInfo(param.getOppositeMetaData());
                StringBuilder sb = new StringBuilder();
                String field_split = param.getValue(
                        ParamKey.fieldSplit, "\\t");
                String line_split = param.getValue(
                        ParamKey.lineSplit, "\\n");
                sb.append("CREATE  TABLE " + tableName + "("
                        + columnInfo + ") ");

                if(isPartitionTable) {
                    sb.append("PARTITIONED BY (" + partitionName + ") ");
                }
                sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
                        + field_split + "' LINES TERMINATED BY '" + line_split
                        + "' STORED AS " + fileType);

                logger.info("create table sql:" + sb.toString());
                logger.info("create external table begin");
                stmt.execute(sb.toString());
                logger.info("create external table end");
            }
            //load data from hdfs
            logger.info("dataLocal:" + dataLocal);
            String loadSql = String.format("load data inpath '%s' into table %s",dataLocal,tableName);
            if(isPartitionTable) {
                loadSql += " PARTITION ("+partitionInfo+")";
            }
            logger.info("load data sql:" + loadSql);
            logger.info("load data begin");

            stmt.execute(loadSql);
            logger.info("load data end");

            /*boolean partitionExist = false;
            String partitionPath = "";
            int partitionNameSize = partitionNames.size();
            for (int i = 0; i < partitionNameSize; i++) {
                partitionPath += partitionNames.get(i) + "="
                        + partitionValues.get(i);
                if(partitionNameSize != i+1) {
                    partitionPath += "/";
                }

            }
            partitionExist = partitionExist(stmt, tableName, partitionPath);
            String partitionInfo = "";
            for (int i = 0; i < partitionNameSize; i++) {

                partitionInfo += partitionNames.get(i) + "='"
                        + partitionValues.get(i) + "'";
                if(i+1 != partitionNameSize) {
                    partitionInfo += ",";
                }
            }
*/

            // add partition
            /*if (!partitionExist) {
                String execution = "ALTER TABLE " + tableName
                        + " ADD PARTITION (" + partitionInfo + ") LOCATION '"
                        + dataLocal + "'";

                logger.info("alter table sql:" + execution);
                logger.info("alter table begin");

                stmt.execute(execution);
                logger.info("alter table end");

            }*/
            switch2HiveTablePost();
        } catch (ClassNotFoundException e) {
            logger.error("Hive Driver Not Found:", e);
            throw new DataExchangeException("Hive Driver Not Found");
        } catch (SQLException e) {
            logger.error("sql failed:",e);
            throw new DataExchangeException("sql failed");
        } catch (Exception e) {
            logger.error("ERROR:",e);
            throw new DataExchangeException();
        } finally {
            try {
                if (res != null)
                    res.close();
                if (stmt != null)
                    stmt.close();
                if (con != null)
                    con.close();
            } catch (SQLException e) {
                logger.warn("SQLException:",e);
            }
        }

        return 0;
    }

    private void setPartitionInfo(List<String> partitionNames,
                                  List<String> partitionValues, PluginParam param, String hdfsLoc) {
        String orgiParNameInfo = param.getValue(
                ParamKey.partitionNames, "").trim();
        String origParValueInfo = param.getValue(
                ParamKey.partitionValues, "").trim();
        boolean partitionAssigned = true;
        boolean partitionNameAssigned = true;
        boolean defaultName = false;
        boolean partitionNameExists = partitionNames.size() > 0 ? true : false;

        if ("".equals(origParValueInfo))
            partitionAssigned = false;
        if ("".equals(orgiParNameInfo))
            partitionNameAssigned = false;
        else if ("pt".equals(orgiParNameInfo) || "ds".equals(orgiParNameInfo))
            defaultName = true;

        // 分区名称给定，分区�1�7�未给定，且给定的分区名称不是默认名秄1�7
        if (partitionNameAssigned && !defaultName) {
            if (!partitionAssigned) {
                logger.error("ERROR：partition value must be specified while partition name is assigned");
                throw new DataExchangeException();
            }
        }
        if (defaultName && partitionNames.size() == 0) {
            partitionNames.add(orgiParNameInfo);
        }

        if (partitionNames.size() == 1) {
            if ("pt".equals(partitionNames.get(0))
                    || "ds".equals(partitionNames.get(0)))
                defaultName = true;
        }


        if (defaultName) {
            Date date = new Date();
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            String partitionValue = df.format(date);
            if ("pt".equals(partitionNames.get(0)))
                partitionValues.add(partitionValue + "000000");
            else if ("ds".equals(partitionNames.get(0)))
                partitionValues.add(partitionValue);
            return;
        }

        int len = partitionNames.size();
        List<String> assignedValues = splitPartitionInfo(origParValueInfo);
        List<String> assignedNames = splitPartitionInfo(orgiParNameInfo);
        if (len > 0) { // 若已存在表且已经存在数据
            if (partitionNameAssigned) { // 用户对分区名称进行了赋�1�7�1�7
                // 无法阻止第一次插入partition时，指定分区多于建表分区的情况，只能sql exception
                if (assignedNames.size() != assignedValues.size()) {
                    logger.error("ERROR: the number of partition name not equals to partition value");
                    throw new DataExchangeException();
                } else if (assignedNames.size() > len) {
                    logger.error("ERROR: the number of partition names assigned is larger than the ones exist");
                    throw new DataExchangeException();
                }
                if (!adjustValueOrder(partitionNames, assignedNames,
                        partitionValues, assignedValues)) {
                    logger.error("ERROR：Need assign value to each partition name");
                    throw new DataExchangeException();
                }

                for (int i = 0; i < assignedValues.size(); i++) {
                    partitionValues.add("");
                }
                //多级分区顺序必须按照已有顺序来，但是指定的时候可以乱序指宄1�7

                for (int i = len - 1, j = assignedValues.size() - 1; i >= 0; i--){
                //判断用户赋�1�7�的分区名称是否与已有名称相匹配
                if(!assignedNames.contains(partitionNames.get(i))) {
                        partitionNames.remove(i); // 若实际有该分区，配置文件中却未指定该分区，则在列表中先删附1�7
                     }else{ // 设置分区对应的�1�7�1�7
                        int idx =
                        assignedNames.indexOf(partitionNames.get(i));
                        partitionValues.set(j, assignedValues.get(idx));
                        j--;
                    }
                }

                return;
            } else if (partitionAssigned) { // 用户未对分区名进行赋值，但对分区值进行赋倄1�7
                if (len != assignedValues.size()) {// 要保证用户对于分区赋值的数量与已有分区名称数量相筄1�7
                    logger.error("ERROR: the number of partition values not equals to the number of existing partitions");
                    throw new DataExchangeException();
                }
                partitionValues.addAll(assignedValues);
                return;
            }
        } else { // 表尚未建立，或虽存在表，但尚未存在数捄1�7
            if (partitionNameAssigned && partitionAssigned) {
                if (assignedNames.size() != assignedValues.size()) {
                    logger.error("ERROR: the number of partition name not equals to partition value");
                    throw new DataExchangeException();
                }
                partitionNames.addAll(assignedNames);
                partitionValues.addAll(assignedValues);
                return;
            }
        }

        String[] paths = hdfsLoc.split("/");
        boolean pathLegal = false;
        assignedNames = new ArrayList<String>();
        assignedValues = new ArrayList<String>();
        for (int subPathIndex = 0; subPathIndex < paths.length; subPathIndex++) {
            String path = paths[subPathIndex];
            int partIndex = path.indexOf("=");
            if (partIndex > 0) {
                if (!partitionNameExists) {
                    partitionNames.add(path.substring(0, partIndex));
                    partitionValues.add(path.substring(partIndex + 1,
                            path.length()));
                } else {
                    assignedNames.add(path.substring(0, partIndex));
                    assignedValues.add(path.substring(partIndex + 1,
                            path.length()));
                }
                pathLegal = true;
            }
        }

        if (!pathLegal) {
            logger.error("ERROR：Need Legal Profile or Legal Path : For example : .../pt=20101101/...");
            throw new DataExchangeException();
        }
        if (partitionNameExists) {
            if (!adjustValueOrder(partitionNames, assignedNames,
                    partitionValues, assignedValues)) {
                logger.error("ERROR：Need assign value to each partition name");
                throw new DataExchangeException();
            }
        }
        return;
    }

    private boolean adjustValueOrder(List<String> partitionNames,
                                     List<String> assignedNames, List<String> partitionValues,
                                     List<String> assignedValues) {
        if (partitionNames.size() > assignedValues.size())
            return false;
        // 多级分区顺序必须按照已有顺序来，但是指定的时候可以乱序指宄1�7
        for (int i = 0; i < partitionNames.size(); i++) {
            if (!assignedNames.contains(partitionNames.get(i))) {
                return false; // 若实际有该分区，配置文件中却未指定该分区，则在列表中先删附1�7
            } else {// 设置分区对应的�1�7�1�7
                int idx = assignedNames.indexOf(partitionNames.get(i));
                partitionValues.add(assignedValues.get(idx));
            }
        }
        return true;
    }

    private List<String> splitPartitionInfo(String info) {
        String[] infos = info.split(",");
        for (int i = 0; i < infos.length; i++)
            infos[i] = infos[i].toLowerCase().trim();
        return Arrays.asList(infos);
    }

    private List<String> getPartitionNames(Statement stmt, String tableName)
            throws SQLException {
        List<String> partitionNames = new ArrayList<String>();
        ResultSet res = stmt.executeQuery("SHOW PARTITIONS " + tableName + "");
        int maxLen = -1;
        String[] partitionInfos = null;
        while (res.next()) {
            String[] maxPar = res.getString(1).split("/");
            if (maxPar.length > maxLen) {
                maxLen = maxPar.length;
                partitionInfos = maxPar;
            }

        }
        if(partitionInfos != null) {
            for (int i = 0; i < partitionInfos.length; i++) {
                int partIndex = partitionInfos[i].indexOf("=");
                partitionNames.add(partitionInfos[i].substring(0, partIndex)
                        .toLowerCase());
            }
        }

        return partitionNames;
    }

    private Connection getHiveConnection(PluginParam param)
            throws ClassNotFoundException, SQLException {
        return HdfsWriterUtil.getHiveConnection(param);
    }

    private String getColumnInfo(MetaData md) {
        return HdfsWriterUtil.getHiveColumnInfo(md);
    }

    private String getHiveStorageFileType(PluginParam param) {
       return HdfsWriterUtil.getHiveStorageFileType(param);
    }

    private String getTableName(PluginParam param) {
       return HdfsWriterUtil.getTableName(param);
    }

    private boolean tableExist(Statement stmt, String tableName)
            throws SQLException {
        return HdfsWriterUtil.tableExist(stmt,tableName);
    }

    private boolean partitionExist(Statement stmt, String tableName,
                                   String partitionPath) throws SQLException {
        ResultSet res = stmt.executeQuery("SHOW PARTITIONS " + tableName);
        while (res.next()) {
            if (partitionPath.equals(res.getString(1))) {
                return true;
            }
        }
        return false;
    }

    private void switch2HiveTablePre() {
        logger.info("execute hive pre begin");
        String hivePre = param.getValue(ParamKey.hivePre,"");
        exeHiveCmd(hivePre);
        logger.info("execute hive pre end");
    }

    private void switch2HiveTablePost() {
        logger.info("execute hive post begin");
        String hivePost = param.getValue(ParamKey.hivePost,"");
        exeHiveCmd(hivePost);
        logger.info("execute hive post end");
    }
    private void exeHiveCmd(String cmds) {
        HdfsWriterUtil.exeHiveCmd(cmds,param);
    }

    public static void main(String args[]) {
        String[] names = StringUtils.split("aa,,ss",',');
        String[] names2 = "aa,,ss".split(",");
        System.out.println(names.length);//2
        System.out.println(names2.length);//3
    }
}
