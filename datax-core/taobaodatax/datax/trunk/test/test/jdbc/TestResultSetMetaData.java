package test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

/**
 * Created by zhuhq on 20i5/i2/23.
 */
public class TestResultSetMetaData {
    public void printColumnInfo(ResultSetMetaData rsmd) throws Exception{

        int count = rsmd.getColumnCount();
        for(int i = 1;i<count;i++) {
            System.out.println("下面这些方法是ResultSetMetaData中方法");
            try {
                System.out.println(i + "列所在的Catalog名字 : ");
                System.out.println(rsmd.getCatalogName(i));
            } catch (Exception e) {
                e.printStackTrace();
            }

                try {
                    System.out.println(i + "列对应数据类型的类 " + rsmd.getColumnClassName(i));
                } catch (Exception ex) {}

                try {
                    System.out.println(i + "列ResultSet所有列的数目 " + rsmd.getColumnCount());
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列在数据库中类型的最大字符个数" + rsmd.getColumnDisplaySize(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列的默认的列的标题" + rsmd.getColumnLabel(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列的模式" + rsmd.getSchemaName(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列的类型,返回SqlType中的编号 " + rsmd.getColumnType(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列在数据库中的类型，返回类型全名" + rsmd.getColumnTypeName(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列类型的精确度(类型的长度): " + rsmd.getPrecision(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列小数点后的位数 " + rsmd.getScale(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列对应的模式的名称（应该用于Oracle） " + rsmd.getSchemaName(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列对应的表名 " + rsmd.getTableName(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列是否自动递增" + rsmd.isAutoIncrement(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列在数据库中是否为货币型" + rsmd.isCurrency(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列是否为空" + rsmd.isNullable(i));
                } catch (Exception ex) {
                }

                try {
                    System.out.println(i + "列是否为只读" + rsmd.isReadOnly(i));
                } catch (Exception ex) {
                }
                try {
                    System.out.println(i + "列能否出现在where中" + rsmd.isSearchable(i));
                } catch (Exception ex) {
                }
            }

    }
    @Test
    public void getOracleColumnInfoTest()  {
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            //new oracle.jdbc.driver.OracleDriver();
            conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.41.225:1521:xe", "dw", "dw");
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select * from DW_MBR_USERINFO_20151009");
            ResultSetMetaData rsmd = rs.getMetaData();
            printColumnInfo(rsmd);
            
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(rs != null) {
                    rs.close();
                    rs = null;
                }
                if(stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if(conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    @Test
    public void getPostgresColumnInfoTest()  {
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            //new oracle.jdbc.driver.OracleDriver();
            conn = DriverManager.getConnection("jdbc:postgresql://192.168.41.225:5500/b2o", "b2o_push", "b2o_push");
            conn.setAutoCommit(false);
            stmt = conn.createStatement();

           // stmt.setFetchSize(1);


            rs = stmt.executeQuery("select * from DW_MBR_USERINFO_20150927 where 1 = 0");


            ResultSetMetaData rsmd = rs.getMetaData();
            printColumnInfo(rsmd);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(rs != null) {
                    rs.close();
                    rs = null;
                }
                if(stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if(conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }

    @Test
    public void getHiveColumnInfoTest()  {
        ResultSet rs = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            //new oracle.jdbc.driver.OracleDriver();
            conn = DriverManager.getConnection("jdbc:hive2://192.168.41.225:10001/default", "", "");
            //conn.setAutoCommit(false);
            stmt = conn.createStatement();

            // stmt.setFetchSize(1);


            rs = stmt.executeQuery("select * from dw_mbr_userinfo_20170199 where 1 = 0");


            ResultSetMetaData rsmd = rs.getMetaData();
            printColumnInfo(rsmd);

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(rs != null) {
                    rs.close();
                    rs = null;
                }
                if(stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if(conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}
