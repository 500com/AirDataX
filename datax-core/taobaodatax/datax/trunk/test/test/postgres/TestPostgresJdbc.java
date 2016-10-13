package test.postgres;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by zhuhq on 2015/11/17.
 */
public class TestPostgresJdbc {
    @Test
    public  void jdbcTest() throws Exception {
        long start = System.currentTimeMillis();
        String username = "b2o_push";
        String password = "b2o_push";
        String ip = "192.168.41.225";
        String port = "5500";
        String dbName = "b2o";
        Class.forName("org.postgresql.Driver");
        String url = String.format("jdbc:postgresql://%s:%s/%s",
                ip,port,dbName);

        Connection connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        statement.setFetchSize(1000);
        ResultSet resultSet = statement.executeQuery("select * from dw_mbr_userinfo_20151009 ");
        try {
            int c = 0;
            while (resultSet.next()) {
                c += 1;
            }
            System.out.println((System.currentTimeMillis() - start) + "ms" + c);
        }finally {
            resultSet.close();
            connection.close();
        }

    }
}
