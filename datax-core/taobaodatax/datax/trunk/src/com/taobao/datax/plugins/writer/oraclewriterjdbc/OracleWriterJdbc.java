package com.taobao.datax.plugins.writer.oraclewriterjdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.plugin.Line;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.MetaData;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.common.util.StrUtils;


import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import oracle.jdbc.OracleTypes;

/**
 * Created by zhuhq on 2016/3/16.
 */
public class OracleWriterJdbc extends Writer {

    /* \001 is field split, \002 is line split */
    private static final char[] replaceChars = { '\001', 0, '\002', 0 };
    private Logger logger = Logger.getLogger(OracleWriterJdbc.class);

    private String password;

    private String username;
    private String host;
    private String port;

    private String dbname;

    private String table;

    private String columns;

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

    private int batchSize = 500;

    private int commit2server;

    private long skipindex;
    private  String url;
    private  String insertSql;
    private  ColumnInfo columnInfo;
    @Override public int init() {
        this.password = param.getValue(ParamKey.password, "");
        this.username = param.getValue(ParamKey.username, "");
        this.host = param.getValue(ParamKey.ip,"localhost");
        this.port = param.getValue(ParamKey.port, "3306");
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
        commit2server = 5000;
        batchSize = param.getIntValue(ParamKey.batchsize,batchSize);
        skipindex = 0;
        logon = username + "/" + password + "@" + dbname;
        url =  param.getValue(ParamKey.url,
                "jdbc:oracle:thin:"+username+"/"+password+"@"+host+":"+port+":" + dbname);
        //columns = param.getValue(ParamKey.columns, StringUtils.join(getColumnNames(),","));
        columnInfo = getColumnInfo();
        columns = StringUtils.join(columnInfo.getColumnNames(),",");
        String sqlValues = columns.replaceAll("[^,]+","?");
        insertSql = "insert into " + table + "("+columns+") values ("+sqlValues+")";
        logger.info("insert sql:" + insertSql);
        logger.info("init complete");
        return PluginStatus.SUCCESS.value();
    }

    @Override public int connect() {
        logger.info("connect complete");
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int startWrite(final LineReceiver receiver) {
        Connection con = null;
        PreparedStatement ps = null;
        Line line = null;
        // List of all date formats that we want to parse.
        // Add your own format here.
        List<SimpleDateFormat>
                dateFormats = new ArrayList<SimpleDateFormat>() {
            private static final long serialVersionUID = 1L;
            {

                add(new SimpleDateFormat("yyyyMMdd"));
                add(new SimpleDateFormat("yyyy-MM-dd"));
                add(new SimpleDateFormat("HH:mm:ss"));
            }
        };
        List<SimpleDateFormat>
                dateTimeFormats = new ArrayList<SimpleDateFormat>() {
            private static final long serialVersionUID = 1L;
            {
                add(new SimpleDateFormat("yyyyMMdd HH:mm:ss"));
                add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            }
        };
        try {
            con = getCon();
            con.setAutoCommit(false);
            ps = con.prepareStatement(insertSql);
            int count = 0;
            String field = null;
            Date date = null;
            while ((line = receiver.getFromReader()) != null) {
                int num = Math.min(line.getFieldNum(), columnInfo.columnNames.length);

                for (int i = 0; i < num; i++) {
                    field = line.getField(i);
                    if(columnInfo.isDate(i)) {
                        int fieldSize = field.length();
                        if(field == null || fieldSize < 1) {
                            ps.setNull(i + 1,columnInfo.getSqlType(i));
                        }else {
                            if(fieldSize > 11) {
                                date = convertToDate(field,dateTimeFormats);
                            }else {
                                date = convertToDate(field,dateFormats);
                            }

                            if(date == null) {
                                ps.setNull(i + 1,columnInfo.getSqlType(i));
                                //ps.setDate(i,null);
                            }else {
                                ps.setDate(i + 1, new java.sql.Date(date
                                        .getTime()));
                            }

                        }
                    }else  {
                        ps.setString(i+1, field);
                    }

                }
                ps.addBatch();
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
                /*if(count % commit2server == 0) {
                    con.commit();
                }*/
            }
            logger.info("counter:"+ count);
            ps.executeBatch(); // insert remaining records
            con.commit();
        } catch (Exception e) {
            logger.error("write data error",e);
            try {
                con.rollback();
            } catch (Exception e1) {
                logger.error("rollback error",e1);
                throw  new DataExchangeException(e1);
            }
        } finally {
            try {
                if (null != ps)
                    ps.close();
                if (null != con)
                    con.close();

            }catch (Exception ignore) {
                logger.error("close resource error", ignore);
            }

        }
        logger.info("startWrite complete");
        return PluginStatus.SUCCESS.value();
    }

    @Override public int commit() {
        logger.info("commit complete");
        return PluginStatus.SUCCESS.value();
    }

    @Override public int finish() {
        logger.info("finish complete");
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public List<PluginParam> split(PluginParam param) {
        OracleWriterSplitter splitter = new OracleWriterSplitter();
        splitter.setParam(param);
        splitter.init();
        logger.info("split complete");
        return splitter.split();
    }

    @Override
    public int prepare(PluginParam param) {
        Connection con = null;
        try {
            if (StringUtils.isNotBlank(pre)) {
                this.logger.info(String
                        .format("OracleWriter starts to execute pre-sql %s .",
                                this.pre));
                con = getCon();
                con.prepareStatement(this.pre).execute();
                con.commit();
            }
        } catch (Exception e) {
            logger.error("prepare error",e);
            throw new DataExchangeException(e);
        }finally {
            if(con != null) {
                try {
                    con.close();
                } catch (Exception ignore) {
                    logger.error("close connection error", ignore);
                }
            }
        }
        logger.info("prepare complete");
        return PluginStatus.SUCCESS.value();
    }
    @Override
    public int post(PluginParam param) {
        Connection conn = null;
        try {
            if (StringUtils.isNotBlank(post)) {
                this.logger.info(String.format(
                        "OracleWriter starts to execute post-sql %s .",
                        this.post));
                conn = getCon();
                conn.createStatement().executeQuery(post);
            }

        } catch (Exception e) {
            logger.error("execute post error",e);
            throw new DataExchangeException(e);
        }finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (Exception ignore) {
                    logger.error("close connection error",ignore);
                }
            }
        }
        logger.info("post complete");
        return PluginStatus.SUCCESS.value();
    }
    private  Connection getCon() {
        Connection connection = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(this.url);

        } catch (Exception e) {
            logger.error("get  connection error", e);
            throw new DataExchangeException(e);
        }
        return connection;
    }
    private ColumnInfo getColumnInfo() {
        Connection con = null;

        String[] columnNames = null;
        Map<Integer,Integer> indexTypeMap = new HashMap<Integer,Integer>();
        try {
            con = getCon();
            PreparedStatement ps = con.prepareStatement("select * from "+table+" where rownum = 1 ");
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            columnNames = new String[columnCount];
            for(int i = 0; i < columnCount; i++){
                columnNames[i] = rsmd.getColumnName(i + 1);
                indexTypeMap.put(i,rsmd.getColumnType(i + 1));
            }
            for(Map.Entry<Integer,Integer> en : indexTypeMap.entrySet()) {
                System.out.println(" indexTypeMap begin columnCount:" + columnCount);
                System.out.print(en.getKey() + ":" + en.getValue() + " ");
                System.out.println(" indexTypeMap end");
            }
        }catch (Exception ex) {
            logger.error("get column names error",ex);
            throw new DataExchangeException(ex);
        }finally {
            try {
                if(con != null) {con.close();}
            } catch (Exception ignore) {
                logger.warn("close connection error", ignore);
            }
        }
        return  new ColumnInfo(columnNames,indexTypeMap);
    }
    private static class ColumnInfo {
        private String[] columnNames = null;
        private Map<Integer,Integer> indexTypeMap;
        public ColumnInfo(String[] columnNames,Map<Integer,Integer>  indexTypeMap) {
            this.columnNames = columnNames;
            this.indexTypeMap = indexTypeMap;
        }
        public String[] getColumnNames() {
            return  columnNames;
        }
        public boolean isDate(int index) {
            if(indexTypeMap == null) {
                System.out.println("columnInfo indexTypeMap is null");
                return false;
            }
            Integer sqlType = indexTypeMap.get(index);
            if(sqlType == null) {
                System.out.println("columnInfo sqlType is null index:" + index);
                return false;
            }
            return  sqlType == OracleTypes.TIMESTAMP ||
                    sqlType == OracleTypes.DATE ||
                    sqlType == OracleTypes.TIME;
        }
        public int getSqlType(int index) {
            return indexTypeMap.get(index);
        }
    }

    /**
     * Convert String with various formats into java.util.Date
     *
     * @param input
     *            Date as a string
     * @return java.util.Date object if input string is parsed
     *          successfully else returns null
     */
    public  Date convertToDate(String input,List<SimpleDateFormat> dateFormats) {
        Date date = null;
        if(input == null || input.length() < 1) {
            return  date;
        }
        input = input.trim();
        for (SimpleDateFormat format : dateFormats) {
            try {
                //format.setLenient(false);
                date = format.parse(input);
            } catch (Exception ignore) {
                //Shhh.. try other formats
            }
            if (date != null) {
                return  date;
            }
        }

        return date;
    }

}
