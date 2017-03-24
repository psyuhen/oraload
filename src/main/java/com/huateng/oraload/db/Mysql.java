package com.huateng.oraload.db;

import com.huateng.oraload.model.DBParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by sam.pan on 2017/3/24.
 */
public class Mysql implements DBInfo{
    private final static Logger LOGGER = Logger.getLogger(Mysql.class);

    private DBParams dbParams;
    public Mysql(DBParams dbParams){
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
        String url = "jdbc:mysql://";
        if(StringUtils.isBlank(dbParams.getUrl())){
            url += dbParams.getIp() + ":" + dbParams.getPort() + "/" + dbParams.getService();
        }else{
            String tmpUrl = dbParams.getUrl();

            final int endIndex = tmpUrl.lastIndexOf(":");
            url += tmpUrl.substring(0, endIndex) + "/" + tmpUrl.substring(endIndex+1);
        }
        LOGGER.info("url==>" + url);

        return url;
    }

    @Override
    public String getDSCls() {
        return "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
    }
}
