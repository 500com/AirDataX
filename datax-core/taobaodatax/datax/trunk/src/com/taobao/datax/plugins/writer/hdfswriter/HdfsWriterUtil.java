package com.taobao.datax.plugins.writer.hdfswriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.taobao.datax.common.exception.DataExchangeException;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.plugin.MetaData;
import com.taobao.datax.common.plugin.PluginParam;
import com.taobao.datax.plugins.common.DBUtils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * Created by zhuhq on 2016/8/12.
 */
public class HdfsWriterUtil {
    private static Logger logger = LoggerFactory.getLogger(HdfsWriterUtil.class);
    public static Connection getHiveConnection(PluginParam param)
            throws ClassNotFoundException, SQLException {
        Connection con;
        String hiveServer = param.getValue(ParamKey.hiveServer,
                "192.168.41.225");
        String hivePort = param.getValue(ParamKey.hiveServerPort,
                "10000");
        String hiveDatabase = param.getValue(ParamKey.hiveDatabase,"default");
        String hiveUsername = param.getValue(ParamKey.hiveUsername,"datax");
        String driverLocal = "jdbc:hive2://" + hiveServer + ":" + hivePort
                + "/" + hiveDatabase;
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Class.forName(driverName);
        con = DriverManager.getConnection(driverLocal, hiveUsername, "");
        return con;
    }

    public static  String getHiveColumnInfo(MetaData md) {
        List<MetaData.Column> columns = md.getColInfo();
        String columnInfo = "";

        for (int i = 0; i < columns.size(); i++) {
            MetaData.Column column = columns.get(i);
            String name = column.getColName();
            String type;
            String typeName = column.getDataType();
            type = "STRING";  //所有字段转为String类型 20160331

            /*if (column.isNum()) {
                type = "BIGINT";
            }
            else if("number".equalsIgnoreCase(typeName) ||
                    "numeric".equalsIgnoreCase(typeName) ||
                    "decimal".equalsIgnoreCase(typeName)) {
                type = "DECIMAL";
            }else if("float".equalsIgnoreCase(typeName)) {
                type = "FLOAT";
            }else if("double".equalsIgnoreCase(typeName)) {
                type = "DOUBLE";
            }else if("TINYINT".equalsIgnoreCase(typeName)) {
                type = "TINYINT";
            }else if("SMALLINT".equalsIgnoreCase(typeName)) {
                type = "SMALLINT";
            }else if("int".equalsIgnoreCase(typeName) || "MEDIUMINT".equals(typeName)) {
                type = "INT";
            }else if("integer".equalsIgnoreCase(typeName)||"bigint".equalsIgnoreCase(typeName)) {
                type = "BIGINT";
            }
            else {
                type = "STRING";
            }*/
            columnInfo += name + " " + type;
            if (i != columns.size() - 1)
                columnInfo += ",";

            logger.info(name +":" +  type + ":" + typeName);
        }
        return columnInfo;
    }

    public static String getHiveStorageFileType(PluginParam param) {
        String fileType = "TEXTFILE";
        String profileType = param.getValue(ParamKey.fileType, "TXT").trim().toUpperCase();
        if ("SEQ".equals(profileType) || "SEQ_COMP".equals(profileType)) {
            fileType = "SEQUENCEFILE";
        } else if ("PARQUET".equals(profileType))  {
            fileType = "PARQUET";
        }
        return fileType;
    }

    public static boolean tableExist(Statement stmt, String tableName)
            throws SQLException {
        boolean isExist = true;
        try {
            stmt.executeQuery("SELECT  * FROM " + tableName + " limit 1");
        }catch (SQLException sqlE) {
            isExist = false;
            logger.warn("check table exist ",sqlE);
        }

        return isExist;
    }

    public static void exeHiveCmd(String cmds,PluginParam param) {
        Connection connection = null;
        try {
            if(StringUtils.isNotEmpty(cmds)) {
                String[] hiveCmds = StringUtils.split(cmds,";");
                connection = getHiveConnection(param);
                Statement statement = connection.createStatement();
                for (String hiveCmd:hiveCmds) {
                    statement.execute(hiveCmd);
                }
            }
        }catch (Exception e) {
            logger.error("exe hive cmds  error:",e);
            throw new DataExchangeException(e);
        }finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {

                }
            }
        }

    }

    public static String getTableName(PluginParam param) {
        String tableName = param.getValue(ParamKey.hiveTableName, null);

        if (tableName == null || "".equals(tableName)) {

            MetaData md = param.getOppositeMetaData();
            if (md == null) {
                logger.error("md is null");
                throw new DataExchangeException("md is null");
            }
            String tmpTableName = md.getTableName();
            char[] tmpTableNameChar = tmpTableName.toCharArray();
            int endLoc = tmpTableNameChar.length - 1;
            while (endLoc >= 0) {
                if (!Character.isDigit(tmpTableNameChar[endLoc])
                        && tmpTableNameChar[endLoc] != '_')
                    break;
                else
                    endLoc--;
            }
            tableName = "s_"
                    + String.copyValueOf(tmpTableNameChar, 0, endLoc + 1)
                    .toLowerCase();
        }
        return tableName;
    }
    public static boolean isSwitch2HiveTable(PluginParam param) {
        return  "true".equalsIgnoreCase(param.getValue(ParamKey.hiveTableSwitch,"false"));
    }
    public static String hiveFieldType2ParquetType(String hiveTypeName) {
        String parquetType = "";
        if("string".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "binary";
        }else if("number".equalsIgnoreCase(hiveTypeName) ||
                "numeric".equalsIgnoreCase(hiveTypeName) ||
                "decimal".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "double";
        }else if("float".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "float";
        }else if("double".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "double";
        }else if("TINYINT".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "int32";
        }else if("SMALLINT".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "int32";
        }else if("int".equalsIgnoreCase(hiveTypeName) || "integer".equalsIgnoreCase(hiveTypeName) || "MEDIUMINT".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "int32";
        }else if("bigint".equalsIgnoreCase(hiveTypeName)) {
            parquetType = "int64";
        }
        else {
            //logger.info("other column type:" + hiveTypeName);
            parquetType = "binary";
        }
        return  parquetType;
    }
    public static MessageType createParquetSchema(PluginParam param) {
        return createParquetSchema(param,null);
    }
    public static MessageType createParquetSchema(PluginParam param,MetaData metaData) {
        String schemaStr = param.getValue(ParamKey.parquetSchema, "");

        if(StringUtils.isBlank(schemaStr) && isSwitch2HiveTable(param)) {
            if(metaData == null) {
                metaData = getHiveTableMetaData(param);
            }
            List<MetaData.Column> columns = metaData.getColInfo();
            StringBuilder sb = new StringBuilder(" message hive_schema { ");

            for(MetaData.Column column : columns) {
                sb.append("optional ");
                String typeName = column.getDataType();
                String columnName = column.getColName();
                int indexOfDot = columnName.toLowerCase().indexOf('.');
                if(indexOfDot >= 0) {
                    columnName = columnName.substring(indexOfDot + 1, columnName.length());
                }
                String parquetType = hiveFieldType2ParquetType(typeName);
                if("binary".equals(parquetType)) {
                    sb.append("binary " + columnName + " (UTF8)");
                }else {
                    sb.append(parquetType + " " + columnName);
                }
                sb.append(";");
            }
            sb.append(" }");
            schemaStr = sb.toString();
            logger.info("schema:" + schemaStr);
        }
        MessageType schema = null;
        if(StringUtils.isNotBlank(schemaStr)) {
            schema = MessageTypeParser.parseMessageType(schemaStr);
        }
        /*String schemaStr = "message hive_schema {\n"
                + "  optional binary ca_address_sk;\n"
                + "  optional binary ca_address_id (UTF8);\n"
                + "  optional binary ca_street_number (UTF8);\n"
                + "  optional binary ca_street_name (UTF8);\n"
                + "  optional binary ca_street_type (UTF8);\n"
                + "  optional binary ca_suite_number (UTF8);\n"
                + "  optional binary ca_city (UTF8);\n"
                + "  optional binary ca_county (UTF8);\n"
                + "  optional binary ca_state (UTF8);\n"
                + "  optional binary ca_zip (UTF8);\n"
                + "  optional binary ca_country (UTF8);\n"
                + "  optional binary ca_gmt_offset;\n"
                + "  optional binary ca_location_type (UTF8);\n"
                + "}";*/
        return schema;
    }


    public static MetaData getHiveTableMetaData(PluginParam param) {
        MetaData metaData = param.getOppositeMetaData();
        if(metaData == null) {
            metaData = param.getMyMetaData();
            if(metaData == null) {
                String tableName = getTableName(param);
                String sql = "select * from " + tableName;
                try {
                    Connection connection = getHiveConnection(param);
                    metaData = DBUtils.genMetaData(connection,sql);
                    param.setMyMetaData(metaData);
                } catch (Exception e) {
                    logger.error("get hive "+ tableName +" metadata error",e);
                }
            }
        }
        return metaData;
    }
}
