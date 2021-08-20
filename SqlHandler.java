package com.paidtech.framework.jdbc;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import com.paidtech.util.ResourceLoader;

/**
 * @author raymond
 * this class provides a base for generic JDBC applications, it has code to
 * manage connections, execute dynamic SQL statements and dispatch the results
 * of updates and selects to one of two methods that must be overridden (use of
 * abstracts there!).
 * It does not use PreparedStatements but a descendant that has a known set of
 * SQL statements could do this by simply overriding runStatement() and adding
 * support for preparing the statement and setting parameters.
 */
public abstract class SqlHandler {
	private Connection dbConn;

	private static final String PROP_FILE_NAME = "sql.properties";
	private static final String DRIVER_FILE_NAME = "dbdrivers.txt";
	public static final String PROP_DB_URL = "dburl";
	private static Properties dbProps;

	/**
	 * when we start up, load the database properties and get a driver loaded
	 */
	static {
		dbProps = new Properties();
		loadProperties();
		setupDrivers();
	}

	/**
	 * @return the classname of the driver that was requested in the props file
	 * @throws IOException 
	 */
	protected static Collections<String> getDriverNames() throws IOException {
		Collections<String> driverNames = new ArrayList<String>();
		BufferedReader df = 
			new BufferedReader(new InputStreamReader(new ResourceLoader("/").locateFile(DRIVER_FILE_NAME)));
		String driverName;
		while ((driverName=df.readLine())!= null) {
			driverNames.add(driverName);
		}
		return driverNames;
	}

	/**
	 * Connect to the database that was specified in the props file
	 * ask the DriverManager to negotiate a connection with the driver that
	 * was specified in the props file.
	 * @throws SQLException
	 */
	protected void connect() throws SQLException {
		dbConn = DriverManager.getConnection(getDBUrl(), dbProps);
	}

	/**
	 * Get the DBURL named in the properties
	 * @return
	 */
	protected String getDBUrl() {
		return dbProps.getProperty(PROP_DB_URL);
	}

	/**
	 * Load the driver (named in the Properties) from disk. Register the loaded
	 * driver with the DriverManager.
	 * This method could iterate over a number
	 * of Driver names, attempting to load and register each of them.
	 */
	private static void setupDrivers() {
		try {
			Collection<String> candidateDriverNames = getDriverNames();
			for ( String candidateDriverName : candidateDriverNames ) {
				System.out.println("driver : "+candidateDriverName);
				Class vendorJDBCDriverClass = 
					Class.forName(candidateDriverName);
			DriverManager.registerDriver(
				(Driver) 
				vendorJDBCDriverClass.newInstance());
			}
		} catch (Exception e) {
			System.err.println("Error initializing database drivers" + e);
			e.printStackTrace(System.err);
		}
	}

	private static Collection<String> getDriverNames() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Load the properties from disk.  The file should define several required
	 * values, and probably user and password for the driver.
	 * The file is expected to reside in "user.dir" or if not there then in the base
	 * directory for the deployed application's class files: for example the root of
	 * the CLASSPATH entry that contains the DAO classes themselves.
	 * @see PROP_DB_DRIVER
	 * @see PROP_DB_URL
	 * @see PROP_FILE_NAME
	 */
	private static void loadProperties() {
		try {
			dbProps  = new ResourceLoader("/").locateResources(PROP_FILE_NAME);
			} catch (IOException e2) {
			System.err.println(
				"Error locating Properties file: "
					+ PROP_FILE_NAME);
			}
	}

	/**
	* If we do not have a connection, get one
	*/
	protected Connection getConnection() throws Exception {
		if (dbConn == null) {
			try {
				connect();
				dbConn.setAutoCommit(false);
			} catch (Exception e) {
				System.err.println(e.getMessage());
				dbConn = null;
				throw e;
			}
		}
		return dbConn;
	}

	public void rollback() throws SQLException {
		if (dbConn != null) {
			dbConn.rollback();
		}		
	}
	
	public void commit() throws SQLException {
		if (dbConn != null) {
			dbConn.commit();
		}
	}

	/**
	 *  Try to close an open DB connection. Ignore any errors
	 */
	public void closeConn() {
		try {
			if (dbConn != null) {
				dbConn.close();
			}
		} catch (Exception e) {
		}
	}

	/**
	 *  Run any sql statement: a Query or an insert/update/delete.  If the statement returned results
	 * display them, otherwise display the number of rows affected.
	 * Note: This code uses the newer/generic "execute" that can handle any
	 * SQL statement in leiu of the update/select specific executeUpdate and executeQuery methods.
	 */
	public void runStatement(String sql) throws Exception {
		getConnection();

		Statement sqlStatement = dbConn.createStatement();

		System.out.println("Executing " + sqlStatement);
		boolean selectQueryExecuted = sqlStatement.execute(sql);
		// is there a ResultSet waiting for us,
		// i.e. was it a SELECT statement?
		if (selectQueryExecuted) {
			handleResults(sqlStatement);
		} else {
			checkRowsAffected(sqlStatement);
		}
		sqlStatement.close();
	}

	protected abstract void handleResults(Statement stmt) throws SQLException;

	protected abstract void checkRowsAffected(Statement stmt)
		throws SQLException;

}