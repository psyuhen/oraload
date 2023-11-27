package com.huateng.oraload.imp;

import com.huateng.oraload.dao.AbstractDAO;
import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.model.Params;
import com.huateng.oraload.pool.ThreadPool;
import com.huateng.oraload.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by sam.pan on 2017/3/6.
 */
@Slf4j
public class Import {
    private final Map<String, Integer> dateTypeMap = new HashMap<>();

    private final Params params;

    public Import(Params params) {
        this.params = params;
    }

    public void imp() {

        if (StringUtils.isBlank(this.params.getTable_name())) {
            log.error("导入的数据表未设置，退出 ==> ");
            System.exit(0);
        }

        if (!StringUtils.isBlank(this.params.getSql())) {
            //String tmpSql = "select * from (" + this.params.getSql() + ") where rownum < 1";
            String[] fields = HikariCPManager.singleQuery2(this.params.getSql(), new AbstractDAO<String[]>() {
                @Override
                public String[] mapping(ResultSet rs) throws SQLException {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int colCount = metaData.getColumnCount();
                    String[] fields = new String[colCount];
                    for (int i = 0; i < colCount; i++) {
                        String colLabel = metaData.getColumnName(i + 1);
                        fields[i] = colLabel;
                        int columnType = metaData.getColumnType(i + 1);
                        if (Types.DATE == columnType) {
                            dateTypeMap.put(i + "", Types.DATE);
                        } else if (Types.TIME == columnType) {
                            dateTypeMap.put(i + "", Types.TIME);
                        } else if (Types.TIMESTAMP == columnType) {
                            dateTypeMap.put(i + "", Types.TIMESTAMP);
                        }
                    }
                    return fields;
                }
            });
            this.params.setFields(fields);
        }

        StringBuilder sb = new StringBuilder(1000);
        sb.append("insert into ");
        sb.append(this.params.getTable_name());
        sb.append(" (");
        StringBuilder sb2 = new StringBuilder(500);
        for (int i = 0, len = this.params.getFields().length; i < len; i++) {
            sb.append(this.params.getFields()[i]);
            dateTypeHandler(i, sb2);
            if (i < len - 1) {
                sb.append(",");
                sb2.append(",");
            }
        }
        sb.append(") values (");
        sb.append(sb2);
        sb.append(") ");
        this.params.setInsertSql(sb.toString());
        log.info("insert sql => " + this.params.getInsertSql());

        if (this.params.getFile() == null) {
            this.params.setFile(new File(this.params.getDest_file()));
        }

        if (!this.params.getFile().exists()) {
            log.error("data file not exist ==> " + this.params.getDest_file());
        }

        readData();
    }

    private void dateTypeHandler(int index, StringBuilder sb2) {
        if (dateTypeMap.containsKey(index + "")) {
            int columnType = dateTypeMap.get(index + "");
            String database = params.getDatabase();
            if ("oracle".equalsIgnoreCase(database)) {
                if (Types.DATE == columnType) {
                    sb2.append("to_date(?, 'yyyymmdd')");
                } else if (Types.TIME == columnType) {
                    sb2.append("to_timestamp(?, 'hh24miss')");
                } else if (Types.TIMESTAMP == columnType) {
                    sb2.append("to_timestamp(?, 'yyyymmddhh24missff3')");
                }
            } else if ("mysql".equalsIgnoreCase(database)) {
                if (Types.DATE == columnType) {
                    sb2.append("str_to_date(?, '%Y%m%d')");
                } else if (Types.TIME == columnType) {
                    sb2.append("str_to_date(?, '%H%i%s')");
                } else if (Types.TIMESTAMP == columnType) {
                    sb2.append("str_to_timestamp(?, '%Y%m%d%H%i%s')");
                }
            }
        } else {
            sb2.append("?");
        }
    }

    private void readData() {
        BufferedReader br = null;
        InputStreamReader isr = null;
        FileInputStream fis = null;
        long startTime = System.currentTimeMillis();
        try {
            fis = new FileInputStream(this.params.getFile());
            isr = new InputStreamReader(fis, this.params.getCharset());
            br = new BufferedReader(isr);

            String item;
            String sqrt = StringUtils.isBlank(this.params.getSqrt()) ? "|" : this.params.getSqrt();
            LinkedList<Object[]> list = new LinkedList<>();
            long totalNum = 0;
            while ((item = br.readLine()) != null) {

                if (StringUtils.endsWith(item, sqrt)) {
                    item = StringUtils.substring(item, 0, item.length() - sqrt.length());
                }
                final String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(item, sqrt);
                list.offer(strings);

                if (list.size() % 1000 == 0) {
                    totalNum += 1000;
                    batchUpdate(list);

                    log.info("inserted " + totalNum + " to db");
                }
            }

            if (!list.isEmpty()) {
                totalNum += list.size();
                batchUpdate(list);
                log.info("inserted " + totalNum + " to db");
            }

            log.info("total write " + totalNum + " row to db ===>");
            log.info("total time :" + (System.currentTimeMillis() - startTime) + "ms");

            //文件读完，关闭
            ThreadPool.getPool().shutdown();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            StreamUtil.close(fis);
            StreamUtil.close(isr);
            StreamUtil.close(br);
        }
    }

    private void batchUpdate(LinkedList<Object[]> list) {
        LinkedList<Object[]> backupList = new LinkedList<>(list);
        int[] update = HikariCPManager.batchExecuteUpdate(this.params.getInsertSql(), list);
        if (update == null) {
            signleUpdate(backupList);
            backupList.clear();
        }
    }

    //多线程处理单条单条处理
    private void signleUpdate(LinkedList<Object[]> list) {
        if (list == null || list.isEmpty()) {
            return;
        }

        log.info("批量操作失败，开始单条插入");
        int size = list.size();
        if (size < 10) {
            for (Object[] obj : list) {
                HikariCPManager.executeUpdate(this.params.getInsertSql(), obj);
            }
            return;
        }

        //启动5个线程的线程池
        ThreadPool threadPool = ThreadPool.getPool();
        for (Object[] obj : list) {
            threadPool.execute(() -> HikariCPManager.executeUpdate(this.params.getInsertSql(), obj));
        }
    }

}
