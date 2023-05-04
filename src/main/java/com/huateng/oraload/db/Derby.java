package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Derby extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:derby://";
    }

    @Override
    public String getDSCls() {
        return "org.apache.derby.jdbc.ClientDataSource";
    }
}
