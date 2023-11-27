package com.huateng.oraload.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
/**
 *
 * @author Administrator
 *
 * @param <E>
 */
public abstract class AbstractDAO<E> {
    /**
     * 回调函数mapping
     * @param rs 结果集
     * @return 返回实体类
     */
    public abstract E mapping(ResultSet rs) throws SQLException;
}
