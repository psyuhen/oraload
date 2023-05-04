package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Hsqldb extends AbstractDataBase{
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:hsqldb:";
    }

    @Override
    public String getDSCls() {
        return "org.hsqldb.jdbc.JDBCDataSource";
    }
}
