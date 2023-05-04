package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class PostgreSql extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:postgresql://";
    }

    @Override
    public String getDSCls() {
        return "org.postgresql.ds.PGSimpleDataSource";
    }
}
