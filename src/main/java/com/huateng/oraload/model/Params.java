package com.huateng.oraload.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.File;

/**
 * Created by sam.pan on 2017/3/7.
 */
@ToString
@Getter @Setter
public class Params {

     // 数据库
    private String database;
     // 导入或卸载表名
    private String table_name;
     //导入或者卸载的文件数据路径（绝对路径）
    private String dest_file;
    //分隔符
    private String sqrt;
    //导入或者卸载的sql
    private String sql;
    //导入或者卸载的文件
    private File file;
    //导入的字段
    private String []fields;
    //插入数据的sql
    private String insertSql;
    //字符编码
    private String charset = "UTF-8";

    private Params(){}

    public static class ParamHolder{
        public static Params params = new Params();
    }

    public static Params getInstance(){
        return ParamHolder.params;
    }
}
