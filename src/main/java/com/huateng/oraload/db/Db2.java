package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Db2 extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:db2://";
    }

    @Override
    public String getDSCls() {
        return "com.ibm.db2.jcc.DB2SimpleDataSource";
    }
}
