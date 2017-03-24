package com.huateng.oraload.db;

import com.huateng.oraload.model.DBParams;

/**
 * Created by sam.pan on 2017/3/24.
 */
public interface DBInfo {
    String getUser();
    String getPwd();
    String getUrl();
    String getDSCls();
}
