package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/3/24.
 */
public class Oracle extends AbstractDataBase{
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:oracle:thin:@";
    }

    @Override
    public String getDSCls() {
        return "oracle.jdbc.pool.OracleDataSource";
    }
}
