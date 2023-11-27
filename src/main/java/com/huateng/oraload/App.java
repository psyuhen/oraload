package com.huateng.oraload;

import com.huateng.oraload.db.AbstractDataBase;
import com.huateng.oraload.db.DataBase;
import com.huateng.oraload.db.Db2;
import com.huateng.oraload.db.Derby;
import com.huateng.oraload.db.H2;
import com.huateng.oraload.db.HikariCPManager;
import com.huateng.oraload.db.Hsqldb;
import com.huateng.oraload.db.Informix;
import com.huateng.oraload.db.Mysql;
import com.huateng.oraload.db.Oracle;
import com.huateng.oraload.db.PostgreSql;
import com.huateng.oraload.db.Sqlite;
import com.huateng.oraload.db.Sqlserver;
import com.huateng.oraload.db.SyBase;
import com.huateng.oraload.imp.Import;
import com.huateng.oraload.model.DBParams;
import com.huateng.oraload.model.Params;
import com.huateng.oraload.unload.Unload;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;

/**
 * Hello world!
 */
@Slf4j
public class App {
    private final Options options = new Options();
    private final String[] args;

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

    private void commandLine() {
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
        options.addOption("db", "database", true, "which database");

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);
            DBParams dbParams = DBParams.getInstance();
            Params params = Params.getInstance();
            if (cmd.hasOption("v")) {
                log.info("OraLoad version:v1.0.0.1");
            } else if (cmd.hasOption("h")) {
                help();
            } else {
                final Iterator iterator = cmd.iterator();
                while (iterator.hasNext()) {
                    final Option next = (Option) iterator.next();
                    final String opt = next.getOpt();
                    final String nextValue = next.getValue();
                    if (StringUtils.equals("u", opt)) {
                        dbParams.setUsername(nextValue);
                        log.info("get username:" + nextValue);
                    } else if (StringUtils.equals("p", opt)) {
                        dbParams.setPassword(nextValue);
                        log.info("get password:" + nextValue);
                    } else if (StringUtils.equals("ip", opt)) {
                        dbParams.setIp(nextValue);
                        log.info("get Ip:" + nextValue);
                    } else if (StringUtils.equals("port", opt)) {
                        dbParams.setPort(nextValue);
                        log.info("get port:" + nextValue);
                    } else if (StringUtils.equals("s", opt)) {
                        dbParams.setService(nextValue);
                        log.info("get service:" + nextValue);
                    } else if (StringUtils.equals("url", opt)) {
                        dbParams.setUrl(nextValue);
                        log.info("get url:" + nextValue);
                    } else if (StringUtils.equals("f", opt)) {
                        params.setDest_file(nextValue);
                        log.info("get dest file:" + nextValue);
                    } else if (StringUtils.equals("sql", opt)) {
                        params.setSql(nextValue);
                        log.info("get sql:" + nextValue);
                    } else if (StringUtils.equals("t", opt)) {
                        params.setTable_name(nextValue);
                        log.info("get table name:" + nextValue);
                    } else if (StringUtils.equals("fd", opt)) {
                        params.setFields(nextValue.split(",", -1));
                        log.info("get fields:" + nextValue);
                    } else if (StringUtils.equals("sqrt", opt)) {
                        params.setSqrt(nextValue);
                        log.info("get sqrt:" + nextValue);
                    } else if (StringUtils.equals("db", opt)) {
                        params.setDatabase(nextValue);
                        log.info("get db:" + nextValue);
                    }
                }
                log.info("DB params:" + dbParams);
                log.info("Other params:" + params);


                if (!StringUtils.isBlank(dbParams.getUsername())) {
                    AbstractDataBase dbInfo = null;
                    final String database = params.getDatabase();
                    if ("mysql".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.MYSQL);
                        dbInfo = new Mysql();
                    } else if ("oracle".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.ORACLE);
                        dbInfo = new Oracle();
                    } else if ("db2".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.DB2);
                        dbInfo = new Db2();
                    } else if ("derby".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.DERBY);
                        dbInfo = new Derby();
                    } else if ("h2".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.H2);
                        dbInfo = new H2();
                    } else if ("hsqldb".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.HSQLDB);
                        dbInfo = new Hsqldb();
                    } else if ("informix".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.INFORMIX);
                        dbInfo = new Informix();
                    } else if ("postgresql".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.POSTGRESQL);
                        dbInfo = new PostgreSql();
                    } else if ("sqlite".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.SQLITE);
                        dbInfo = new Sqlite();
                    } else if ("sqlserver".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.SQLSERVER);
                        dbInfo = new Sqlserver();
                    } else if ("sybase".equalsIgnoreCase(database)) {
                        dbParams.setDatabase(DataBase.SYBASE);
                        dbInfo = new SyBase();
                    } else {
                        log.error("UNKNOWN DATABASE:" + database);
                        System.exit(1);
                    }
                    dbInfo.setDbParams(dbParams);
                    HikariCPManager.resetConnection(dbInfo);
                }

                if (cmd.hasOption("unload")) {
                    log.info("unload start ==>");
                    Unload unload = new Unload(params);
                    unload.toUnload();
                } else if (cmd.hasOption("imp")) {
                    log.info("import start ==>");
                    Import importData = new Import(params);
                    importData.imp();
                } else {
                    help();
                }
            }
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
            help();
        }
    }

    private void help() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("java com.huateng.oraload.App [-options]\r\noptions:\r\n", options);
        System.exit(0);
    }
}
