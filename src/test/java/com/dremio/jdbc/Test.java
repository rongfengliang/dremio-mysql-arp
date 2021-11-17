package com.dremio.jdbc;

import com.dremio.exec.store.jdbc.CloseableDataSource;
import org.junit.Ignore;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {

    @org.junit.Test
    @Ignore
    public void init() throws SQLException {

        DataSource myDataSource = new DataSource();
        myDataSource.hostname = "localhost";
        myDataSource.username = "sqluser01";
        myDataSource.port="2881";
        myDataSource.password = "123456";
        try(CloseableDataSource closeableDataSource = myDataSource.newDataSource()){
            ResultSet resultSet = closeableDataSource.getConnection().createStatement().executeQuery("select * from test.appdemo");
            while (resultSet.next()) {
                String info = resultSet.getString(1);
                System.out.println(info);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
