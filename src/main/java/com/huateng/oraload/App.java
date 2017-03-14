package com.huateng.oraload;

import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.imp.ImportData;
import com.huateng.oraload.model.DBParams;
import com.huateng.oraload.model.Params;
import com.huateng.oraload.unload.Unload;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;

/**
 * Hello world!
 */
public class App {
    private static final Log LOGGER = LogFactory.getLog(App.class);
    private Options options = new Options();
    private String[] args = null;

    public App(String[] args) {
        this.args = args;
    }
    public static void main(String[] args) {

//        String sql = "select * from sys_module";
        /*String sql = "select * from sys_datadictionary";
        String destFile = "e:/sys_datadictionary.dat";
        Unload unload = new Unload(sql, destFile);
        unload.toUnload();*/

        /*String destFile = "e:/sys_datadictionary.dat";
        String sql = "select * from sys_datadictionary1";
        ImportData importData = new ImportData();
        importData.setTable_name("sys_datadictionary1");
        importData.setDestFile(destFile);
        importData.setSql(sql);
        importData.imp();*/

        App app = new App(args);
        app.commandLine();
    }

    private void commandLine(){
        options.addOption("h", "help", false, "command help info");
        options.addOption("v", "version", false, "version info");
        options.addOption("p", "password", true, "DataBase password");
        options.addOption("u", "user", true, "DataBase user");
        options.addOption("ip", "ip", true, "DataBase ip address");
        options.addOption("port", "port", true, "DataBase port");
        options.addOption("s", "service", true, "DataBase service name Or Sid");
        options.addOption("f", "file", true, "unload or import Data file");
        options.addOption("sql", "sql", true, "unload select sql or import table sql");
        options.addOption("t", "table_name", true, "import table name");
        options.addOption("fd", "field", true, "import field name,eg:f1,f2,f3");
        options.addOption("sqrt", "sqrt", true, "unload or import data sqrt,eg: 11|bb|");
        options.addOption("url", "url", true, "DataBase url, eg: localhost:1521:testdb");
        options.addOption("unload", "unload", false, "unload start");
        options.addOption("imp", "imp", false, "import start");

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);
            DBParams dbParams = DBParams.getInstance();
            Params params = Params.getInstance();
            if(cmd.hasOption("v")){
                LOGGER.info("OraLoad version:v1.0.0.1");
            }else if(cmd.hasOption("h")){
                help();
            }else{
                final Iterator iterator = cmd.iterator();
                while(iterator.hasNext()){
                    final Option next = (Option)iterator.next();
                    final String opt = next.getOpt();
                    final String nextValue = next.getValue();
                    if(StringUtils.equals("u", opt)){
                        dbParams.setUsername(nextValue);
                        LOGGER.info("get username:" + nextValue);
                    }else if(StringUtils.equals("p", opt)){
                        dbParams.setPassword(nextValue);
                        LOGGER.info("get password:" + nextValue);
                    }else if(StringUtils.equals("ip", opt)){
                        dbParams.setIp(nextValue);
                        LOGGER.info("get Ip:" + nextValue);
                    }else if(StringUtils.equals("port", opt)){
                        dbParams.setPort(nextValue);
                        LOGGER.info("get port:" + nextValue);
                    }else if(StringUtils.equals("s", opt)){
                        dbParams.setService(nextValue);
                        LOGGER.info("get service:" + nextValue);
                    }else if(StringUtils.equals("url", opt)){
                        dbParams.setUrl(nextValue);
                        LOGGER.info("get url:" + nextValue);
                    }else if(StringUtils.equals("f", opt)){
                        params.setDest_file(nextValue);
                        LOGGER.info("get dest file:" + nextValue);
                    }else if(StringUtils.equals("sql", opt)){
                        params.setSql(nextValue);
                        LOGGER.info("get sql:" + nextValue);
                    }else if(StringUtils.equals("t", opt)){
                        params.setTable_name(nextValue);
                        LOGGER.info("get table name:" + nextValue);
                    }else if(StringUtils.equals("fd", opt)){
                        params.setFields(nextValue.split(",", -1));
                        LOGGER.info("get fields:" + nextValue);
                    }else if(StringUtils.equals("sqrt", opt)){
                        params.setSqrt(nextValue);
                        LOGGER.info("get sqrt:" + nextValue);
                    }
                }
                LOGGER.info("DB params:" + dbParams);
                LOGGER.info("Other params:" + params);


                if(!StringUtils.isBlank(dbParams.getUsername())){
                    HikariCPManager.resetConnection(dbParams);
                }

                if(cmd.hasOption("unload")){
                    LOGGER.info("unload start ==>");
                    Unload unload = new Unload(params);
                    unload.toUnload();
                }else if(cmd.hasOption("imp")){
                    LOGGER.info("import start ==>");
                    ImportData importData = new ImportData(params);
                    importData.imp();
                }else{
                    help();
                }
            }
        } catch (ParseException e) {
            LOGGER.error(e.getMessage(),e);
            help();
        }
    }

    private void help(){
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java com.huateng.oraload.App [-options]\r\noptions:\r\n", options);
        System.exit(0);
    }
}
