package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class H2 extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:h2:";
    }

    @Override
    public String getDSCls() {
        return "org.h2.jdbcx.JdbcDataSource";
    }
}
