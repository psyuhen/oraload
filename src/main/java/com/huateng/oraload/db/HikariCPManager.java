package com.huateng.oraload.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.huateng.oraload.model.DBParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.huateng.oraload.dao.AbstractDAO;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * 采用HikariCP类库来管理
 * 数据库管理类
 * @author ps
 */
public class HikariCPManager {
	private final static Logger LOGGER = Logger.getLogger(HikariCPManager.class);
	private static HikariDataSource ds;
//	private static final String PROPERTIES = "hikaricp.properties";

	/*static {
		Properties pro = new Properties();
		try {
			pro.load(HikariCPManager.class.getClassLoader().getResourceAsStream(PROPERTIES));
			HikariConfig config = new HikariConfig(pro);
			ds = new HikariDataSource(config);
		} catch (IOException e) {
			LOGGER.error(PROPERTIES+" is load error",e);
		}
	}*/

	public static void resetConnection(DBParams dbParams){
		if(ds != null){
			ds = null;
		}

		try{
			Properties properties = new Properties();
			properties.setProperty("dataSourceClassName", "oracle.jdbc.pool.OracleDataSource");
			String url = "jdbc:oracle:thin:@";
			if(StringUtils.isBlank(dbParams.getUrl())){
				url += dbParams.getIp() + ":" + dbParams.getPort() + ":" + dbParams.getService();
			}else{
				url += dbParams.getUrl();
			}
			LOGGER.info("url==>" + url);
			properties.setProperty("dataSource.url", url);
			properties.setProperty("dataSource.user", dbParams.getUsername());
			properties.setProperty("dataSource.password", dbParams.getPassword());
			HikariDataSource bds = new HikariDataSource(new HikariConfig(properties));

			ds = bds;
		}catch (Exception e){
			LOGGER.error(e.getMessage(),e);
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
		Connection conn = null;
		try {
			conn = ds.getConnection();
		} catch (SQLException e) {
			LOGGER.error("获取连接异常", e);
			throw new IllegalStateException(e);
		}
		return conn;
	}

	/**
	 * 查询SQL，并返回一个list数据集
	 *
	 * @param <E>
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 数据集
	 */
	public static <E> List<E> executeQuery(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			List<E> result = new LinkedList<E>();
			while (rs.next()) {
				result.add(dao.mapping(rs));
			}
			return result;
		} catch (SQLException e) {
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("rs关闭异常", e);
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				LOGGER.error("statement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 查询SQL，并返回一个list数据集
	 *
	 * @param <E>
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
		Connection connection = null;
		try {
			connection = getConnection();
			ps = connection.prepareStatement(sql);

			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			rs = ps.executeQuery();
			List<E> result = new ArrayList<E>();
			while (rs.next()) {
				result.add(dao.mapping(rs));
			}
			return result;
		} catch (SQLException e) {
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("ResultSet关闭异常", e);
			}
			try {
				if (ps != null)
					ps.close();
			} catch (Exception e) {
				LOGGER.error("PreparedStatement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param <E>
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery(String sql, Object[] params, AbstractDAO<E> dao) {
		PreparedStatement  ps = null;
		ResultSet rs = null;
		Connection connection = null;
		try {
			connection = getConnection();
			ps = connection.prepareStatement(sql);
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
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("ResultSet关闭异常", e);
			}
			try {
				if (ps != null)
					ps.close();
			} catch (Exception e) {
				LOGGER.error("PreparedStatement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}
	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param <E>
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			if (rs.next()) {
				return dao.mapping(rs);
			} else {
				return null;
			}
		} catch (SQLException e) {
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("rs关闭异常", e);
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				LOGGER.error("statement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 查询SQL，并返回一条数据
	 *
	 * @param <E>
	 * @param sql
	 *            查询SQL
	 * @param dao
	 *            回调函数
	 * @return 一条数据,若没有数据，返回null
	 */
	public static <E> E singleQuery2(String sql, AbstractDAO<E> dao) {
		Statement stmt = null;
		ResultSet rs = null;
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			return dao.mapping(rs);
		} catch (SQLException e) {
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("rs关闭异常", e);
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				LOGGER.error("statement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 查询SQL，从firstRow到MaxRow的数据
	 *
	 * @param <E>
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
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			rs = stmt.executeQuery(sql);
			int CurrentRow = 0;
			List<E> result = new ArrayList<E>();
			while (rs.next()) {
				CurrentRow++;
				if (CurrentRow < firstRow) {
					continue;
				} else if (CurrentRow >= maxRow) {
					break;
				} else {
					E obj = dao.mapping(rs);
					result.add(obj);
				}
			}
			return result;
		} catch (SQLException e) {
			LOGGER.error("查询异常，查询语句[" + sql + "]", e);
			return null;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception e) {
				LOGGER.error("rs关闭异常", e);
			}
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception e) {
				LOGGER.error("statement关闭异常", e);
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 执行一个SQL,如：insert,update,delete,select等
	 *
	 * @param sql
	 * @return true if the first result is a ResultSet object; false if it is an
	 *         update count or there are no results
	 */
	public static boolean execute(String sql) {
		Statement stmt = null;
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			return stmt.execute(sql);
		} catch (SQLException e) {
			LOGGER.error("SQL异常,SQL语句[" + sql + "]", e);
			return false;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					LOGGER.error("statement关闭异常", e);
				}
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 *         statements or (2) 0 for SQL statements that return nothing
	 */
	public static int executeUpdate(String sql) {
		Statement stmt = null;
		Connection connection = null;
		try {
			connection = getConnection();
			stmt = connection.createStatement();
			return stmt.executeUpdate(sql);
		} catch (SQLException e) {
			LOGGER.error("SQL异常,SQL语句[" + sql + "]", e);
			return 0;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					LOGGER.error("statement关闭异常", e);
				}
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql
	 * @param params
	 *            参数
	 * @return either (1) the row count for SQL Data Manipulation Language (DML)
	 *         statements or (2) 0 for SQL statements that return nothing
	 */
	public static int executeUpdate(String sql, Object[] params) {
		PreparedStatement ps = null;
		Connection connection = null;
		try {
			connection = getConnection();
			ps = connection.prepareStatement(sql);

			if (params != null) {
				for (int i = 0; i < params.length; i++) {
					ps.setObject(i + 1, params[i]);
				}
			}

			connection.setAutoCommit(false);// 设置不自动提交
			int count = ps.executeUpdate();
			connection.commit();

			return count;
		} catch (SQLException e) {
			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException e1) {
				LOGGER.error("回滚出错", e1);
			}
			LOGGER.error("SQL异常,SQL语句[" + sql + "]", e);
			return 0;
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					LOGGER.error("PreparedStatement关闭异常", e);
				}
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}

	/**
	 * 执行一个DML的SQL，如insert,update,delete等
	 *
	 * @param sql
	 * @param paramList
	 *            参数
	 * @return an array of update counts containing one element for each command
	 *         in the batch. The elements of the array are ordered according to
	 *         the order in which commands were added to the batch
	 */
	public static int[] batchExecuteUpdate(String sql, LinkedList<Object[]> paramList) {
		PreparedStatement ps = null;
		Connection connection = null;
		try {
			connection = getConnection();
			ps = connection.prepareStatement(sql);

			if (paramList != null) {
				Object[] item = null;
				while((item = paramList.poll()) != null){
					for (int j = 0; j < item.length; j++) {
						ps.setObject(j + 1, item[j]);
					}
					ps.addBatch();
				}
			}

			connection.setAutoCommit(false);// 设置不自动提交
			int[] count = ps.executeBatch();
			connection.commit();

			return count;
		} catch (SQLException e) {
			try {
				if (connection != null) {
					connection.rollback();
				}
			} catch (SQLException e1) {
				LOGGER.error("回滚出错", e1);
			}
			LOGGER.error("SQL异常,SQL语句[" + sql + "]", e);
			return null;
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					LOGGER.error("PreparedStatement关闭异常", e);
				}
			}
			try {
				if (connection != null)
					connection.close();
			} catch (Exception e) {
				LOGGER.error("connection关闭异常", e);
			}
		}
	}
}
