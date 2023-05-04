package com.huateng.oraload.db;

import com.huateng.oraload.model.DBParams;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.commons.lang3.StringUtils;

/**
 * 数据库的基类
 * Created by sam.pan on 2017/9/22.
 */
@CommonsLog
public abstract class AbstractDataBase implements DBInfo{
    /**
     * 数据库的jdbc参数
     */
    private @Getter @Setter DBParams dbParams;
    /**
     * <pre>
     * JDBC 的URL前缀
     * 如：ORACLE: jdbc:oracle:thin:@
     * Mysql: jdbc:mysql://
     * </pre>
     */
    private @Getter @Setter String jdbcUrlPrefix;

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
        String jdbcUrlPrefix = getJdbcUrlPrefix();
        StringBuffer jdbcUrl = new StringBuffer(50);
        //如果没有填写url选项，默认增加
        if(StringUtils.isBlank(dbParams.getUrl())){
            jdbcUrl.append(jdbcUrlPrefix).append(dbParams.getIp()).append(":").append(dbParams.getPort());
            if(dbParams.getDatabase() == DataBase.ORACLE){
                jdbcUrl.append(":");
            }else{
                jdbcUrl.append("/");
            }
            jdbcUrl.append(dbParams.getService());
        }else{
            //不包含jdbc:形状的，自动添加
            if(!StringUtils.startsWith(dbParams.getUrl(), "jdbc:")){
                jdbcUrl.append(jdbcUrlPrefix);
            }
            jdbcUrl.append(dbParams.getUrl());
        }
        log.info("url==>" + jdbcUrl.toString());

        return jdbcUrl.toString();
    }
}
