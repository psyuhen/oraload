package com.huateng.oraload.db;

/**
 * Created by sam.pan on 2017/9/22.
 */
public class Sqlite extends AbstractDataBase {
    @Override
    public String getJdbcUrlPrefix() {
        return "jdbc:sqlite:";
    }

    @Override
    public String getDSCls() {
        return "org.sqlite.SQLiteDataSource";
    }
}
