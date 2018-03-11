package com.aqj.etl.test.dao;

import com.mysql.cj.jdbc.Driver;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public class MysqlDaoSupport extends JdbcDaoSupport {

    private static SimpleDriverDataSource dataSource = new SimpleDriverDataSource();

    public static void setDataSource(){
        dataSource.setDriverClass(Driver.class);
        dataSource.setPassword("");
        dataSource.setUrl("");
        dataSource.setUsername("");
    }

    public MysqlDaoSupport(){
        setDataSource();
    }




}
