package com.huateng.oraload.db;

import com.huateng.oraload.model.DBParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by sam.pan on 2017/3/24.
 */
public class Oracle implements DBInfo{
    private final static Logger LOGGER = Logger.getLogger(Oracle.class);
    private DBParams dbParams;
    public Oracle(DBParams dbParams){
        this.dbParams = dbParams;
    }

    @Override
    public String getPwd() {
        return dbParams.getPassword();
    }

    @Override
    public String getUser() {
        return dbParams.getUsername();
    }

    @Override
    public String getUrl() {
        String url = "jdbc:oracle:thin:@";
        if(StringUtils.isBlank(dbParams.getUrl())){
            url += dbParams.getIp() + ":" + dbParams.getPort() + ":" + dbParams.getService();
        }else{
            url += dbParams.getUrl();
        }
        LOGGER.info("url==>" + url);

        return url;
    }

    @Override
    public String getDSCls() {
        return "oracle.jdbc.pool.OracleDataSource";
    }
}
