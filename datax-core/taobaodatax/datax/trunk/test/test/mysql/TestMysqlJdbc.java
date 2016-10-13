package test.mysql;

import com.taobao.datax.plugins.common.DBUtils;
import org.junit.Test;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by zhuhq on 2015/11/12.
 */
public class TestMysqlJdbc {

    @Test
    public  void jdbcTest() throws Exception {
        String username = "salesmanager";
        String password = "salesmanager";
        String ip = "192.168.30.177";
        String port = "3306";
        String dbName = "SALESMANAGER";
        Class.forName("com.mysql.jdbc.Driver");
        String url = String.format("jdbc:mysql://%s:%s/%s?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8",
                ip,port,dbName);

        Connection connection = DriverManager.getConnection(url, username, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select name from country_description where COUNTRY_ID = 15 and LANGUAGE_ID = 1 ");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
