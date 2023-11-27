package com.huateng.oraload.unload;

import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.model.Params;
import com.huateng.oraload.util.StreamUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by sam.pan on 2017/3/6.
 */
@Slf4j
public class Unload {
    private final Params params;

    public Unload(Params params) {
        this.params = params;
    }

    public void toUnload() {
        if (this.params.getFile() == null || !this.params.getFile().exists()) {
            this.params.setFile(new File(this.params.getDest_file()));
        }

        this.params.setSqrt(StringUtils.isBlank(this.params.getSqrt()) ? "|" : this.params.getSqrt());
        log.info("unload data starting ===>");
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        final int MAX_FETCH_SIZE = 1000;
        long totalNum = 0;
        try {
            conn = HikariCPManager.getConnection();
            long startTime = System.currentTimeMillis();
            ps = conn.prepareStatement(this.params.getSql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            rs.setFetchSize(MAX_FETCH_SIZE);
            LinkedList<String> rowList = new LinkedList<>();
            StringBuilder sb = new StringBuilder(1000);
            while (rs.next()) {
                rs2Row(sb, rs);

                rowList.add(sb.toString());
                sb.setLength(0);
                int rowNum = rs.getRow();
                if (rowNum % MAX_FETCH_SIZE == 0) {
                    writeFile(rowList);
                    totalNum += MAX_FETCH_SIZE;
                    log.info("writed {}  ===> file", totalNum);
                }
            }

            if (!rowList.isEmpty()) {
                int lastCount = rowList.size();
                writeFile(rowList);
                totalNum += lastCount;
                log.info("writed {}  ===> file", totalNum);
            }


            log.info("writed {}  ===> file", totalNum);
            log.info("total time :{}ms", (System.currentTimeMillis() - startTime));
        } catch (SQLException e) {
            log.error("连接数据库出错！");
            throw new RuntimeException("连接数据库出错！", e);
        } finally {
            StreamUtil.close(rs);
            StreamUtil.close(ps);
            StreamUtil.close(conn);
        }
    }

    private String rs2Row(ResultSet rs) {
        String curRow = "";
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();
            StringBuilder sb = new StringBuilder(1000);
            for (int i = 0; i < colCount; i++) {
                String colLabel = metaData.getColumnName(i + 1);
                String colValue = rs.getString(colLabel);
                colValue = (colValue == null) ? "" : colValue;
                sb.append(colValue);
                sb.append(Unload.this.params.getSqrt());
            }
            curRow = sb.toString();
        } catch (SQLException e) {
            log.error("获取表的列信息出错:{}", e.getMessage());
        }

        return curRow;
    }

    private void rs2Row(StringBuilder sb, ResultSet rs) {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();
            for (int i = 0; i < colCount; i++) {
                String colValue = rs.getString(i + 1);
                colValue = (colValue == null) ? "" : colValue;
                sb.append(colValue);
                sb.append(Unload.this.params.getSqrt());
            }
        } catch (SQLException e) {
            log.error("获取表的列信息出错:{}", e.getMessage());
        }

    }

    /**
     * 把数据写到文件中
     *
     * @param list 数据List
     */
    private void writeFile(LinkedList<String> list) {
        BufferedWriter bw = null;
        OutputStreamWriter osw = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(this.params.getFile(), true);
            osw = new OutputStreamWriter(fos, params.getCharset());
            bw = new BufferedWriter(osw);

            String item;
            while ((item = list.poll()) != null) {
                bw.write(item);
                bw.newLine();
            }
            bw.flush();
        } catch (FileNotFoundException e) {
            log.error("{}文件找不到", this.params.getFile().getName());
        } catch (IOException e) {
            log.error("写入文件失败:{}", e.getMessage());
        } finally {
            StreamUtil.close(fos);
            StreamUtil.close(osw);
            StreamUtil.close(bw);
        }
    }
}
