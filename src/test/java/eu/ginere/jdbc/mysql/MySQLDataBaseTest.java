package eu.ginere.jdbc.mysql;

import java.util.List;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import eu.ginere.base.util.test.TestResult;

public class MySQLDataBaseTest extends TestCase {
	static final Logger log = Logger.getLogger(MySQLDataBaseTest.class);
	
	
	@Test
	static public void testMain() throws Exception {
		try {
			setDataSource();

			TestResult rest=MySQLDataBase.DEFAULT_DATABASE.test();
			log.debug(rest);
			assertTrue(rest.isOK());
			
			List <String> databases=MySQLDataBase.DEFAULT_DATABASE.getDatabases();
			
			log.debug("Databases:"+StringUtils.join(databases, ","));
			
			String BDNAME="junit_DB_TEST";
            
			if (MySQLDataBase.DEFAULT_DATABASE.exitsDatabase(BDNAME)){
                MySQLDataBase.DEFAULT_DATABASE.dropDatabase(BDNAME);
            }

			assertFalse(MySQLDataBase.DEFAULT_DATABASE.exitsDatabase(BDNAME));

			MySQLDataBase.DEFAULT_DATABASE.createDatabase(BDNAME);
			assertTrue(MySQLDataBase.DEFAULT_DATABASE.exitsDatabase(BDNAME));			

			// TODO THIS DOES NOT WORK, WHY ???
			MySQLDataBase.DEFAULT_DATABASE.useDatabase(BDNAME);
            String selectedDatabase=MySQLDataBase.DEFAULT_DATABASE.getSelectedDatabase();
            log.debug("Selected Database:"+selectedDatabase);
			assertNotNull(selectedDatabase);			

			MySQLDataBase.DEFAULT_DATABASE.dropDatabase(BDNAME);
			assertFalse(MySQLDataBase.DEFAULT_DATABASE.exitsDatabase(BDNAME));
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}
	private static void setDataSource() throws Exception {
		String filePropertiesName="conf/jdbc.properties";
		
//		DataSource dataSource = MySQLDatabaseUtils.createMySQLDataSourceFromPropertiesFile(filePropertiesName);
//		MySQLDataBase.initDatasource(filePropertiesName,dataSource);

		MySQLDatabaseUtils.initDatasource(filePropertiesName);

	}
}
