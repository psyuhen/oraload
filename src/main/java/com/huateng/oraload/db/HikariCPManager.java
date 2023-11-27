package com.huateng.oraload.db;

import com.huateng.oraload.dao.AbstractDAO;
import com.huateng.oraload.util.StreamUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * 采用HikariCP类库来管理
 * 数据库管理类
 * @author ps
 */
@Slf4j
public class HikariCPManager {
	private static HikariDataSource ds;

	/**
	 * 重置数据库连接池
	 * @param dbInfo 数据库详细信息
	 */
	public static void resetConnection(DBInfo dbInfo){
		if(ds != null){
			ds.close();
			ds = null;
		}

		try{
			Properties properties = new Properties();
			properties.setProperty("dataSourceClassName", dbInfo.getDSCls());
			properties.setProperty("dataSource.url", dbInfo.getUrl());
			properties.setProperty("dataSource.user", dbInfo.getUser());
			properties.setProperty("dataSource.password", dbInfo.getPwd());
			HikariConfig hikariConfig = new HikariConfig(properties);

			ds = new HikariDataSource(hikariConfig);
		}catch (Exception e){
			log.error("初始化数据库连接异常,出错原因：{}", e.getMessage());
			log.error("", e);
			System.exit(1);
		}
	}

	/**
	 * 获取数据库连接
	 *
	 * @author yanping.wang
	 * @return connection
	 */
	public static Connection getConnection() {
		Connection conn;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			log.error("获取数据库连接异常,出错原因：{}", e.getMessage());
			throw new IllegalStateException(e);
		}
		return conn;
	}

	/**
	 * 查询SQL，并返回一个list数据集
	 *
	 * @param <E> 泛型变量
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 数据集
	 */
	public static <E> List<E> executeQuery(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			List<E> result = new LinkedList<>();
			while (rs.next()) {
				result.add(dao.mapping(rs));
			}
			return result;
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 查询SQL，并返回一个list数据集
	 *
	 * @param <E> 泛型变量
	 * @param sql
	 *            查询SQL
	 * @param params
	 *            参数
	 * @param dao
	 *            回调函数
	 * @return 数据集
	 */
	public static <E> List<E> executeQuery(String sql, Object[] params,
										   AbstractDAO<E> dao) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);

			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			rs = ps.executeQuery();
			List<E> result = new ArrayList<>();
			while (rs.next()) {
				result.add(dao.mapping(rs));
			}
			return result;
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(ps);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param <E> 泛型变量
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery(String sql, Object[] params, AbstractDAO<E> dao) {
		PreparedStatement  ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			rs = ps.executeQuery();
			if (rs.next()) {
				return dao.mapping(rs);
			} else {
				return null;
			}
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(ps);
			StreamUtil.close(conn);
		}
	}
	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param <E> 泛型变量
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next()) {
				return dao.mapping(rs);
			} else {
				return null;
			}
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param  <E>泛型变量
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery2(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			return dao.mapping(rs);
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 查询SQL，从firstRow到MaxRow的数据
	 *
	 * @param <E>泛型变量
	 * @param sql
	 *            查询SQL
	 * @param firstRow
	 *            起始行
	 * @param maxRow
	 *            结束行
	 * @param dao
	 *            回调函数
	 * @return 数据集
	 */
	public static <E> List<E> executeQuery(String sql, int firstRow,
										   int maxRow, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			int currentRow = 0;
			List<E> result = new ArrayList<>();
			while (rs.next()) {
				currentRow++;
				if (currentRow < firstRow) {
					continue;
				}

				if (currentRow >= maxRow) {
					break;
				} else {
					E obj = dao.mapping(rs);
					result.add(obj);
				}
			}
			return result;
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(rs);
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 执行一个SQL,如：insert,update,delete,select等
	 *
	 * @param sql SQL语句
	 * @return true if the first result is a ResultSet object; false if it is an
	 *         update count or there are no results
	 */
	public static boolean execute(String sql) {
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			return stmt.execute(sql);
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return false;
		} finally {
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql SQL语句
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 *         statements or (2) 0 for SQL statements that return nothing
	 */
	public static int executeUpdate(String sql) {
		Statement stmt = null;
		Connection conn = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			return stmt.executeUpdate(sql);
		} catch (SQLException e) {
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return 0;
		} finally {
			StreamUtil.close(stmt);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql SQL语句
	 * @param params
	 *            参数
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 *         statements or (2) 0 for SQL statements that return nothing
	 */
	public static int executeUpdate(String sql, Object[] params) {
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);

			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			conn.setAutoCommit(false);// 设置不自动提交
			int count = ps.executeUpdate();
			conn.commit();

			return count;
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error("回滚出错", e1);
			}

			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			if (params != null) {
				log.error("data:{},{},{}",params[0],params[1],params[2] );
			}
			return 0;
		} finally {
			StreamUtil.close(ps);
			StreamUtil.close(conn);
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql SQL语句
	 * @param paramList
	 *            参数
	 * @return an array of update counts containing one element for each command
	 *         in the batch. The elements of the array are ordered according to
	 *         the order in which commands were added to the batch
	 */
	public static int[] batchExecuteUpdate(String sql, LinkedList<Object[]> paramList) {
		PreparedStatement ps = null;
		Connection conn = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);

			if (paramList != null) {
				Object[] item;
				while((item = paramList.poll()) != null){
					for (int j = 0; j < item.length; j++) {
						ps.setObject(j + 1, item[j]);
					}
					ps.addBatch();
				}
			}

			conn.setAutoCommit(false);// 设置不自动提交
			int[] count = ps.executeBatch();
			conn.commit();

			return count;
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (SQLException e1) {
				log.error("回滚出错", e1);
			}
			log.error("SQL异常,SQL语句{},出错原因：{}", sql, e.getMessage());
			return null;
		} finally {
			StreamUtil.close(ps);
			StreamUtil.close(conn);
		}
	}
}
