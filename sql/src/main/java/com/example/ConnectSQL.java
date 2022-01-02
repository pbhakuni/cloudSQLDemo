package com.example;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * This class connects with Cloud SQl postgres instance and performs DML operations
 *
 */
public class ConnectSQL 
{
  
    public static void main( String[] args ) throws Exception
    {
        final String INSTANCE_CONNECTION_NAME =  args[0];   
        final String DB_PASS = args[1];
        final String DB_NAME = args[2];

        //String jdbcURL = String.format("jdbc:postgresql:///%s?cloudSqlInstance=%s&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=%s&password=%s", DB_NAME, INSTANCE_CONNECTION_NAME, DB_USER, DB_PASS);
        String jdbcURL = String.format("jdbc:postgresql:///%s", DB_NAME);
        Properties connProps = new Properties();
        connProps.setProperty("user", "cloud-access@project-learning-335905.iam");
        connProps.setProperty("password", DB_PASS);
        connProps.setProperty("sslmode", "disable");
        connProps.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
        connProps.setProperty("cloudSqlInstance", INSTANCE_CONNECTION_NAME);
        connProps.setProperty("enableIamAuth", "true");
        
        // Initialize connection pool
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(org.postgresql.Driver.class.getName());
        config.setJdbcUrl(jdbcURL);
        config.setDataSourceProperties(connProps);
        config.setConnectionTimeout(10000); // 10s

        HikariDataSource connectionPool = new HikariDataSource(config);
        createTable(connectionPool);
        updateRowsTable(connectionPool);
        deleteRowsTable(connectionPool);

    }

    public static void createTable(DataSource pool) throws SQLException {
        
        System.out.println("------- Started creating table in PostgreSql -------");

        try (Connection conn = pool.getConnection()) {
          String stmt = "Create Table empdetails(id int primary key, name varchar, address text);" +
                        "insert into empdetails values(1, 'Sam', 'ABC');" +
                        "insert into empdetails values(2, 'Steve', 'DEF');";

          try (PreparedStatement createTableStatement = conn.prepareStatement(stmt);) {
            createTableStatement.execute();
          }
        }
        System.out.println("------- Table created!! -------");
      }

      public static void updateRowsTable(DataSource pool) throws SQLException {
        
        System.out.println("------- Started updating rows in table -------");

        try (Connection conn = pool.getConnection()) {
          String stmt = "Update empdetails SET name = 'Priyanka' where name = 'Sam'";

          try (PreparedStatement createTableStatement = conn.prepareStatement(stmt);) {
            createTableStatement.execute();
          }
        }
        System.out.println("------- Rows updated!! -------");
      }

      public static void deleteRowsTable(DataSource pool) throws SQLException {
        
        System.out.println("------- Started deletion of rows in table -------");

        try (Connection conn = pool.getConnection()) {
          String stmt = "delete from empdetails where name = 'Priyanka'";

          try (PreparedStatement createTableStatement = conn.prepareStatement(stmt);) {
            createTableStatement.execute();
          }
        }
        System.out.println("------- Rows deleted!! -------");
      }
}
