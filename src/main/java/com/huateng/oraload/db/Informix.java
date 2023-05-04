package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Informix extends AbstractDataBase{
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:informix-sqli://";
    }

    @Override
    public String getDSCls() {
        return "com.informix.jdbcx.IfxDataSource";
    }
}
