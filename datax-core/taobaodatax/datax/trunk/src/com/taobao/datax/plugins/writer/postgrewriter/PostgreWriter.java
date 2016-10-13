package com.taobao.datax.plugins.writer.postgrewriter;

/**
 * Created by yuanw on 2015/11/5.
 */

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.LineReceiver;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.common.plugin.PluginStatus;
import com.taobao.datax.common.plugin.Writer;
import com.taobao.datax.plugins.common.DBSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class PostgreWriter extends Writer {

    private static List<String> encodingConfigs = null;

    static {
        encodingConfigs = new ArrayList<String>();
        encodingConfigs.add("client_encoding");
    }

    private static Map<String, String> encodingMaps = null;
    static {
        encodingMaps = new HashMap<String, String>();
        encodingMaps.put("utf-8", "UTF8");
    }


    private Logger logger = Logger.getLogger(PostgreWriter.class);

   /* private Connection connection;*/

    private String ip;

    private String port = "5432";

    private String password;

    private String username;

    private String dbname;

    private String table;

    private String pre;

    private String post;

    private char sep = '\001';

    private String set = "";


    private double limit = 0;

    private String encoding;


    private int concurrency;

   /* private String connectKey;*/

    private int lineCounter = 0;

    @Override
    public int prepare(PluginParam param) {
        /*if (!StringUtils.isBlank(this.ip) && !StringUtils.isBlank(this.port)) {
			*//* User defined ip & port *//*
            this.connectKey = DBSource.genKey(this.getClass(), this.ip, this.port, this.dbname, username, password);
            DBSource.register(this.connectKey, this.createProperties());
            this.connection = DBSource.getConnection(this.connectKey);

        }
        if (null == this.connection) {
            String msg = "PostgreWriter try to connect to database failed .";
            logger.error(msg);
            throw new DataExchangeException(msg);
        }*/

        if (StringUtils.isBlank(this.pre))
            return PluginStatus.SUCCESS.value();

        Statement stmt = null;
        Connection connection = null;
        try {
            connection = getCon();
            stmt = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            for (String subSql : this.pre.split(";")) {
                this.logger.info(String.format("Excute prepare sql %s .",
                        subSql));
                stmt.execute(subSql);
            }

            return PluginStatus.SUCCESS.value();
        } catch (Exception e) {
            logger.error("error",e);
            throw new DataExchangeException(e);
        } finally {
            try {
                if (null != stmt) {
                    stmt.close();
                }
                if (null != connection) {
                    connection.close();
                    connection = null;
                }
            } catch (Exception ignore) {
            }
        }


    }

    @Override
    public int post(PluginParam param) {
        /*if (!StringUtils.isBlank(this.ip) && !StringUtils.isBlank(this.port)) {
			*//* User defined ip & port *//*
            this.connectKey = DBSource.genKey(this.getClass(), this.ip, this.port, this.dbname, username, password);
            DBSource.register(this.connectKey, this.createProperties());
            this.connection = DBSource.getConnection(this.connectKey);

        }
        if (null == this.connection) {
            String msg = "PostgreWriter try to connect to database failed .";
            logger.error(msg);
            throw new DataExchangeException(msg);
        }*/

        if (StringUtils.isBlank(this.post))
            return PluginStatus.SUCCESS.value();

        Statement stmt = null;
        Connection connection = null;
        try {
            connection = getCon();
            stmt = connection.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            for (String subSql : this.post.split(";")) {
                this.logger.info(String.format("Excute post sql %s .",
                        subSql));
                stmt.execute(subSql);
            }

            return PluginStatus.SUCCESS.value();
        } catch (Exception e) {
            throw new DataExchangeException(e.getCause());
        } finally {
            try {
                if (null != stmt) {
                    stmt.close();
                }
                if (null != connection) {
                    connection.close();
                    connection = null;
                }
            } catch (Exception ignore) {
            }

        }

    }

    @Override
    public int init() {
        this.username = param.getValue( ParamKey.username, "");
        this.password = param.getValue( ParamKey.password, "");
        this.ip = param.getValue( ParamKey.ip);
        this.port = param.getValue( ParamKey.port, "5500");
        this.dbname = param.getValue( ParamKey.dbname);
        this.table = param.getValue( ParamKey.table);
        this.pre = param.getValue( ParamKey.pre, "");
        this.post = param.getValue( ParamKey.post, "");
        this.encoding = param.getValue( ParamKey.encoding, "UTF8")
                .toLowerCase();
        this.limit = param.getDoubleValue( ParamKey.limit, 0);
        this.concurrency=param.getIntValue(ParamKey.concurrency, 1);
        this.set = param.getValue( ParamKey.set, "");
        this.sep = param.getCharValue(ParamKey.fieldSplit,this.sep);

       /* this.connectKey = DBSource.genKey(this.getClass(), ip, port,
                dbname, username, password);*/

        if (!StringUtils.isBlank(this.set)) {
            this.set = "set " + this.set;
        }

        if (encodingMaps.containsKey(this.encoding)) {
            this.encoding = encodingMaps.get(this.encoding);
        }

        return PluginStatus.SUCCESS.value();

    }

    @Override
    public int connect() {
        /*connection = DBSource.getConnection(this.connectKey);*/
        return PluginStatus.SUCCESS.value();
    }


    @Override
    public int startWrite(LineReceiver resultHandler) {
       Statement stmt = null;
        PostgreWriterInputStreamAdapter localInputStream = null;
        Connection connection = null;
        try {

            connection = getCon();

            stmt = connection.createStatement();

		    //设置编码方式
            this.logger.info(String.format("Config encoding %s .",
                    this.encoding));
            for (String sql : this.makeLoadEncoding(encoding))
                stmt.execute(sql);

			/* load data begin */
            String loadSql = this.makeLoadSql();
            this.logger
                    .info(String.format("Load sql: %s.", visualSql(loadSql)));

            localInputStream = new PostgreWriterInputStreamAdapter(resultHandler, this);
            CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
            copyManager.copyIn(loadSql,localInputStream);

            this.lineCounter = localInputStream.getLineNumber();

            this.logger.info("DataX write to postgre ends .");

            return PluginStatus.SUCCESS.value();
        } catch (Exception e2) {

            logger.error("copy data to postgre error:",e2);
            throw new DataExchangeException(e2.getCause());
        } finally {
            if (null != stmt)
                try {
                    stmt.close();
                    localInputStream.close();
                } catch (Exception e3) {
            }
            if (null != connection) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public int commit() {
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int finish() {

        return PluginStatus.SUCCESS.value();
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
    private Properties createProperties() {
        Properties p = new Properties();
        String url = "jdbc:postgresql://" + this.ip + ":" + this.port + "/"
                + this.dbname;

        p.setProperty("driverClassName", "org.postgresql.Driver");
        p.setProperty("url", url);
        p.setProperty("username", this.username);
        p.setProperty("password", this.password);

        p.setProperty("maxActive", String.valueOf(this.concurrency + 2));
        p.setProperty("initialSize", String.valueOf(this.concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        p.setProperty("defaultReadOnly", "false");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1");

        this.logger.info(String.format("PostgreWriter try connection: %s .", url));
        return p;
    }


    private String visualSql(String sql) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("\n", "\\n");
        map.put("\t", "\\t");
        map.put("\r", "\\r");
        map.put("\\", "\\\\");

        for (String s : map.keySet()) {
            sql = sql.replace(s, map.get(s));
        }
        return sql;
    }



    private String makeLoadSql() {
        String sql = "COPY " + this.table + " FROM STDIN WITH  DELIMITER '"+this.sep+"'";// ENCODING '"+this.encoding+"'";
        return sql;
    }

    private List<String> makeLoadEncoding(String encoding) {
        List<String> ret = new ArrayList<String>();

        String configSql = "SET %s=%s; ";
        for (String config : encodingConfigs) {
            this.logger.info(String.format(configSql, config, encoding));
            ret.add(String.format(configSql, config, encoding));
        }

        return ret;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return ip;
    }

    public void setHost(String host) {
        this.ip = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }



    public String getPre() {
        return pre;
    }

    public void setPre(String pre) {
        this.pre = pre;
    }

    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public double getLimit() {
        return limit;
    }

    public void setLimit(double limit) {
        this.limit = limit;
    }

    public char getSep() {
        return sep;
    }

    public void setSep(char sep) {
        this.sep = sep;
    }

    public static void  main(String args[]) throws Exception {
            Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://" + "192.168.41.225" + ":" + "5500" + "/"
                + "b2o";
        PGConnection connection = (PGConnection)DriverManager.getConnection(url, "b2o_push", "b2o_push");
        CopyManager copyManager = connection.getCopyAPI();
        copyManager.copyIn("copy zhuhq_test from STDIN  WITH  DELIMITER '\001' ",new FileInputStream(new File("d:\\test.txt")));

    }
}
