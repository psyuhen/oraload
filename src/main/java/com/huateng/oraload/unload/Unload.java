package com.huateng.oraload.unload;

import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.model.Params;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.*;
import java.util.LinkedList;

/**
 * Created by sam.pan on 2017/3/6.
 */
public class Unload {
    private static final Log LOGGER = LogFactory.getLog(Unload.class);

    /*private @Getter @Setter String sql;
    private @Getter @Setter File file;
    private @Getter @Setter String destFile;
    private @Getter @Setter String sqrt;*/
    private Params params;

    public Unload(Params params){
        this.params = params;
    }

    public void toUnload(){
        if(this.params.getFile() == null || !this.params.getFile().exists()){
            this.params.setFile(new File(this.params.getDest_file()));
        }

        this.params.setSqrt(StringUtils.isBlank(this.params.getSqrt())?"|" : this.params.getSqrt());
        LOGGER.info("unload data starting ===>");
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int MAX_FETCH_SIZE = 1000;
        long totalNum = 0;
        long startTime = System.currentTimeMillis();
        try {
            conn = HikariCPManager.getConnection();
            ps = conn.prepareStatement(this.params.getSql(),ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            rs = ps.executeQuery();
            rs.setFetchSize(MAX_FETCH_SIZE);
            LinkedList<String> rowList = new LinkedList<String>();
            while(rs.next()){
                String curRow = rs2Row(rs);

                rowList.add(curRow);
                int rowNum = rs.getRow();
                if(rowNum % MAX_FETCH_SIZE == 0){
                    writeFile(rowList);
                    totalNum += MAX_FETCH_SIZE;
                    LOGGER.info("writed " +totalNum+ " row to file ===>");
                }
            }

            if(!rowList.isEmpty()){
                int lastCount = rowList.size();
                writeFile(rowList);
                totalNum += lastCount;
                LOGGER.info("writed " +totalNum+ " row to file ===>");
            }


            LOGGER.info("total write " +totalNum+ " row to file ===>");
            LOGGER.info("total time :" + (System.currentTimeMillis() - startTime) + "ms");
        } catch (SQLException e) {
            LOGGER.error("连接数据库出错！");
            throw new RuntimeException("连接数据库出错！",e);
        } finally{
            try {
                if(rs != null){
                    rs.close();
                }
            } catch (SQLException e) {
                LOGGER.error("关闭查询结果集出错！");
            }
            try {
                if(ps != null){
                    ps.close();
                }
            } catch (SQLException e) {
                LOGGER.error("关闭prepareStatement出错！");
            }
            try {
                if(conn != null){
                    conn.close();
                }
            } catch (SQLException e) {
                LOGGER.error("关闭数据库连接出错！");
            }
        }
    }

    private String rs2Row(ResultSet rs){
        String curRow = "";
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int colCount = metaData.getColumnCount();
            StringBuilder sb = new StringBuilder(1000);
            for (int i = 0; i < colCount; i++) {
                String colLabel = metaData.getColumnName(i+1);
                String colValue = rs.getString(colLabel);
                colValue = (colValue == null) ? "" : colValue;
                sb.append(colValue);
                sb.append(Unload.this.params.getSqrt());
            }
            curRow = sb.toString();
        } catch (SQLException e) {
            LOGGER.error("获取表的列信息出错");
        }

        return curRow;
    }

    private void writeFile(LinkedList<String> list){
        BufferedWriter bw = null;
        OutputStreamWriter osw = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(this.params.getFile(), true);
            osw = new OutputStreamWriter(fos, "UTF-8");
            bw = new BufferedWriter(osw);

            String item = null;
            while((item = list.poll()) != null){
                bw.write(item);
                bw.newLine();
            }
            bw.flush();
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage(),e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                if (osw != null) {
                    osw.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
