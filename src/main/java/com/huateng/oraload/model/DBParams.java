package com.huateng.oraload.model;

import com.huateng.oraload.db.DataBase;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by sam.pan on 2017/3/7.
 */
@ToString
public class DBParams {
    private @Getter @Setter DataBase database; // 数据库
    private @Getter @Setter String password; // 密码
    private @Getter @Setter String username; // 用户名
    private @Getter @Setter String service; // SID或者service name
    private @Getter @Setter String ip; // ip
    private @Getter @Setter String port; // port
    private @Getter @Setter String url; // url : eg: localhost:1521:testdb

    private DBParams(){}

    private static DBParams dbParams = null;

    public static DBParams getInstance(){
        if (dbParams == null){
            synchronized (DBParams.class){
                if (dbParams == null){
                    dbParams = new DBParams();
                }
            }
        }
        return dbParams;
    }
}
