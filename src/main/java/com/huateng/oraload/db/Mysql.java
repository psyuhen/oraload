package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/3/24.
 */
public class Mysql extends AbstractDataBase{

    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:mysql://";
    }

    @Override
    public String getDSCls() {
        return "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
    }
}
