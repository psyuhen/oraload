package com.huateng.oraload.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.File;

/**
 * Created by sam.pan on 2017/3/7.
 */
@ToString
public class Params {

    private @Getter @Setter String database; // 数据库
    private @Getter @Setter String table_name; // 导入或卸载表名
    private @Getter @Setter String dest_file; //导入或者卸载的文件数据路径（绝对路径）
    private @Getter @Setter String sqrt;//分隔符
    private @Getter @Setter String sql;//导入或者卸载的sql
    private @Getter @Setter File file;//导入或者卸载的文件
    private @Getter @Setter String []fields;//导入的字段
    private @Getter @Setter String insertSql;//插入数据的sql
    private Params(){}

    private static Params params = null;

    public static Params getInstance(){
        if (params == null){
            synchronized (Params.class){
                if (params == null){
                    params = new Params();
                }
            }
        }
        return params;
    }
}
