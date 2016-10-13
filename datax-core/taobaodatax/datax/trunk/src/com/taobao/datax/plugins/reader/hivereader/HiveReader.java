package com.taobao.datax.plugins.reader.hivereader;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.*;
import com.taobao.datax.plugins.common.DBResultSetSender;
import com.taobao.datax.plugins.common.DBSource;
import com.taobao.datax.plugins.common.DBUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhuhq on 2015/11/24.
 */
public class HiveReader extends Reader {
    private static final Logger logger = Logger.getLogger(HiveReader.class);
    private String hiveServerIp="192.168.41.225";
    private String hiveServerPort="10000";
    private String database="default";
    private String tables;
    private String username="";
    private String password="";
    private String where="";
    private String columns="*";
    private String sql = "";
    private int concurrency=1;

    private Connection conn;
    private String datasourceKey;
    @Override
    public int init() {
        this.hiveServerIp = param.getValue(ParamKey.ip,hiveServerIp);
        this.hiveServerPort=param.getValue(ParamKey.port,hiveServerPort);
        this.database = param.getValue(ParamKey.dbname,database);
        this.tables = param.getValue(ParamKey.tables,"");
        this.username = param.getValue(ParamKey.username,username);
        this.password = param.getValue(ParamKey.password,password);
        this.where = param.getValue(ParamKey.where,where);
        this.columns = param.getValue(ParamKey.columns,columns);
        this.concurrency = param.getIntValue(ParamKey.concurrency,concurrency);
        this.sql = param.getValue(ParamKey.sql,sql);
       /* Properties p = createProperties();
        datasourceKey = DBSource.genKey(this.getClass(), this.hiveServerIp, this.hiveServerPort, this.database);
        DBSource.register(datasourceKey, p);*/
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int connect() {

        conn = getHiveConnection();
        return PluginStatus.SUCCESS.value();
    }

    @Override
    public int startRead(LineSender lineSender) {
        DBResultSetSender proxy = DBResultSetSender.newSender(lineSender);
        proxy.setMonitor(getMonitor());
        proxy.setDateFormatMap(genDateFormatMap());

        String sql = param.getValue(ParamKey.sql);
        logger.info(String.format("HiveReader start to query %s .", sql));
        ResultSet rs = null;
        try {
            rs = DBUtils.query(conn, sql);

            proxy.sendToWriter(rs);
            proxy.flush();
            getMonitor().setStatus(PluginStatus.READ_OVER);

            return PluginStatus.SUCCESS.value();
        } catch (SQLException e) {
            logger.error("exe sql error",e);
            throw new DataExchangeException(e);
        } finally {
            if (null != rs) {
                DBUtils.closeResultSet(rs);
            }
        }
    }

    @Override
    public int finish() {
        try {
            if (conn != null) {
                conn.close();
            }
            conn = null;

        } catch (SQLException e) {
        }
        return PluginStatus.SUCCESS.value();
    }
    private Connection getHiveConnection(){
        Connection con;
        String url = String.format("jdbc:hive2://%s:%s/%s",hiveServerIp,hiveServerPort,database);

        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
            con = DriverManager.getConnection(url, username, password);
        }catch (Exception e) {
            logger.error("get hive connection error",e);
            throw new DataExchangeException(e);
        }

        return con;
    }
    private Properties createProperties() {
        Properties p = new Properties();
        String url = String.format("jdbc:hive2://%s:%s/%s",hiveServerIp,hiveServerPort,database);

        p.setProperty("driverClassName", "org.apache.hive.jdbc.HiveDriver");
        p.setProperty("url", url);
        p.setProperty("username", username);
        p.setProperty("password", password);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));
        p.setProperty("initialSize", String.valueOf(concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");
        p.setProperty("defaultReadOnly", "true");
        p.setProperty("testOnBorrow", "true");
        p.setProperty("validationQuery", "select 1");

        logger.info(String.format("HiveReader try connection: %s .", url));
        return p;
    }
    private Map<String, SimpleDateFormat> genDateFormatMap() {
        Map<String, SimpleDateFormat> mapDateFormat = new HashMap<String, SimpleDateFormat>();
        mapDateFormat.clear();
        mapDateFormat.put("datetime", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss"));
        mapDateFormat.put("timestamp", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss"));
        mapDateFormat.put("time", new SimpleDateFormat("HH:mm:ss"));
        return mapDateFormat;
    }
    @Override
    public List<PluginParam> split(PluginParam param){
        List<PluginParam> sqlList;

        if (StringUtils.isBlank(this.sql)) {
			/* non-user-defined sql */
            HiveReaderSplitter splitter = new HiveReaderSplitter(param);
            splitter.init();
            sqlList = splitter.split();
        } else {
			/* user-define sql */
            sqlList = super.split(param);
        }

        String sql = sqlList.get(0).getValue(ParamKey.sql);
        MetaData m = null;
        Connection conn = null;
        try {
            conn = getHiveConnection();
            m = DBUtils.genMetaData(conn, sql);
            param.setMyMetaData(m);
        } catch (SQLException e) {
            logger.error(ExceptionTracker.trace(e));
            throw new DataExchangeException(e);
        }finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


        }

        return sqlList;
    }
}
