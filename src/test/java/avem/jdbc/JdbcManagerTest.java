package avem.jdbc;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import eu.ginere.base.util.test.TestResult;
import eu.ginere.jdbc.mysql.MySQLDataBase;
import eu.ginere.jdbc.mysql.MySQLDatabaseUtils;

public class JdbcManagerTest extends TestCase {
	static final Logger log = Logger.getLogger(JdbcManagerTest.class);
	
	
	@Test
	static public void testMain() throws Exception {
		try {
			setDataSource();

			TestResult rest=MySQLDataBase.DEFAULT_DATABASE.test();
			log.debug(rest);
			assertTrue(rest.isOK());
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}
	private static void setDataSource() throws Exception {
//		DataSource dataSource = MySQLDatabaseUtils.createMySQLDataSourceFromPropertiesFile("/etc/cgps/jdbc.properties");
//		MySQLDataBase.initDatasource(filePropertiesName,dataSource);
		String filePropertiesName="conf/jdbc.properties";
		DataSource dataSource = MySQLDatabaseUtils.createMySQLDataSourceFromPropertiesFile(filePropertiesName);
		MySQLDataBase.initDatasource(filePropertiesName,dataSource);		
	}
}
