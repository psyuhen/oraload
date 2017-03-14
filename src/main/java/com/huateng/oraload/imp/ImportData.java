package com.huateng.oraload.imp;

import com.huateng.oraload.dao.AbstractDAO;
import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.model.Params;
import com.huateng.oraload.unload.Unload;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by sam.pan on 2017/3/6.
 */
public class ImportData {
    private static final Log LOGGER = LogFactory.getLog(Unload.class);

    /*private @Getter @Setter String table_name;
    private @Getter @Setter String []fields;
    private @Getter @Setter String sql;
    private @Getter @Setter String destFile;
    private @Getter @Setter File file;
    private @Getter @Setter String sqrt;
    private @Getter @Setter String insertSql;*/

    private Params params;

    public ImportData(Params params){
        this.params = params;
    }

    public void imp(){

        if(StringUtils.isBlank(this.params.getTable_name())){
            LOGGER.error("table name is not set ==> ");
            System.exit(0);
        }

        if(!StringUtils.isBlank(this.params.getSql())){
            String tmpSql = "select * from (" + this.params.getSql() + ") where rownum < 1";
            String []fields = HikariCPManager.singleQuery2(this.params.getSql(), new AbstractDAO<String[]>() {
                @Override
                public String[] mapping(ResultSet rs) throws SQLException {
                    ResultSetMetaData metaData = rs.getMetaData();
                    int colCount = metaData.getColumnCount();
                    String [] fields = new String[colCount];
                    for (int i = 0; i < colCount; i++) {
                        String colLabel = metaData.getColumnName(i+1);
                        fields[i] = colLabel;
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
        for (int i = 0,len = this.params.getFields().length; i < len; i++) {
            sb.append(this.params.getFields()[i]);
            sb2.append("?");
            if(i < len -1){
                sb.append(",");
                sb2.append(",");
            }
        }
        sb.append(") values (");
        sb.append(sb2.toString());
        sb.append(") ");
        this.params.setInsertSql(sb.toString());
        LOGGER.info("insert sql => " + this.params.getInsertSql());

        if(this.params.getFile() == null){
            this.params.setFile(new File(this.params.getDest_file()));
        }

        if(!this.params.getFile().exists()){
            LOGGER.error("data file not exist ==> " + this.params.getDest_file());
        }

        readData();
    }

    private void readData(){
        BufferedReader br = null;
        InputStreamReader isr = null;
        FileInputStream fis = null;
        long startTime = System.currentTimeMillis();
        try {
            fis = new FileInputStream(this.params.getFile());
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);

            String item = null;
            String sqrt = StringUtils.isBlank(this.params.getSqrt())? "|" : this.params.getSqrt();
            LinkedList<Object[]> list = new LinkedList<Object[]>();
            long totalNum = 0;
            while((item = br.readLine()) != null){

                if(StringUtils.endsWith(item, sqrt)){
                    item = StringUtils.substring(item, 0, item.length()-sqrt.length());
                }
                final String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(item, sqrt);
                list.offer(strings);

                if(list.size() % 1000 == 0){
                    totalNum += 1000;
                    HikariCPManager.batchExecuteUpdate(this.params.getInsertSql(), list);
                    LOGGER.info("inserted " +totalNum+ " to db");
                }
            }

            if(!list.isEmpty()){
                totalNum += list.size();
                HikariCPManager.batchExecuteUpdate(this.params.getInsertSql(), list);
                LOGGER.info("inserted " +totalNum+ " to db");
            }

            LOGGER.info("total write " +totalNum+ " row to db ===>");
            LOGGER.info("total time :" + (System.currentTimeMillis() - startTime) + "ms");

        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage(),e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(),e);
        }finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                if (isr != null) {
                    isr.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

}
