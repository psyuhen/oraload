package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Sqlserver extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:sqlserver://";
    }

    @Override
    public String getDSCls() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDataSource";
    }
}
