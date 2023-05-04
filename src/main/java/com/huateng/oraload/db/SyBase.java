package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class SyBase extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:jtds:sybase://";
    }

    @Override
    public String getDSCls() {
        return "com.sybase.jdbc4.jdbc.SybDataSource";
    }
}
