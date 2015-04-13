package avem.jdbc.dao.cache;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import eu.ginere.jdbc.mysql.dao.AbstractSQLDAO;
import eu.ginere.jdbc.mysql.dao.util.DBProperties;
import avem.jdbc.MySQLDatabaseUtils;

public class DBPropertiesTest extends TestCase {
	static final Logger log = Logger.getLogger(DBPropertiesTest.class);
	
	private static void setDataSource() throws Exception {
		DataSource dataSource = MySQLDatabaseUtils.createMySQLDataSourceFromPropertiesFile("/etc/cgps/jdbc.properties");
		
		MySQLDatabaseUtils.setDataSource(dataSource);
	}

	@Test
	static public void testBackEnd() throws Exception {	
		try {
			setDataSource();
			AbstractSQLDAO DAO=DBProperties.DAO;
				
			int codeVersion=DAO.getCodeVersion();
			log.info("codeVersion:"+codeVersion);
	
			int installedVersion=DAO.getInstalledVersion();
			log.info("installedVersion:"+installedVersion);
	
			boolean ok=DAO.isBackendOk();
			log.info("isBackendOk:"+ok);
	
			if (!ok){
				DAO.createorUpdateBackEnd();
				log.info("updates: OK");
			} else {
				log.info("is updated.");
	
			}
	
			long elementNumber=DAO.getBackendElementNumber();
			log.info("elementNumber:"+elementNumber);
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}
	
	
	@Test
	static public void testConsulta() throws Exception {
		try {
			setDataSource();

			long elementNumber=DBProperties.DAO.getBackendElementNumber();
			log.info("elementNumber:"+elementNumber);
			
			String value="test";
			String value2;
			String name="test";
			
			if (DBProperties.exists(DBPropertiesTest.class,name)){
				DBProperties.delete(DBPropertiesTest.class,name);
			}
			// insert
			DBProperties.setStringValue(DBPropertiesTest.class,name,value);
			value2=DBProperties.getStringValue(DBPropertiesTest.class, name, null);
			
			assertEquals(value, value2);
			
			// update
			DBProperties.setStringValue(DBPropertiesTest.class, name, "test2");
			value2=DBProperties.getStringValue(DBPropertiesTest.class, name, null);
			assertEquals("test2", value2);
			
			// false
			DBProperties.setStringValue(DBPropertiesTest.class, name, "test3");
			value2=DBProperties.getStringValue(DBPropertiesTest.class, name, null);
			assertFalse( "test2".equals(value2) );
			
			
			// delete
			
			DBProperties.delete(DBPropertiesTest.class,name);
			

		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

}
