package avem.jdbc;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

public class JdbcManagerTest extends TestCase {
	static final Logger log = Logger.getLogger(JdbcManagerTest.class);
	
	
	@Test
	static public void testMain() throws Exception {
		try {
			setDataSource();

			boolean rest=JdbcManager.testConnection();

			assertTrue(rest);
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

	private static void setDataSource() throws Exception {
		DataSource dataSource = JdbcManager.createMySQLDataSourceFromPropertiesFile("/etc/cgps/jdbc.properties");
		
		JdbcManager.setDataSource(dataSource);
	}
}
