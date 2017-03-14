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
     * @param rs
     * @return
     * @throws SQLException
     */
    public abstract E mapping(ResultSet rs) throws SQLException;
}
