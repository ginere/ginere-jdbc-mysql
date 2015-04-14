package eu.ginere.jdbc.mysql.dao.util;

import org.junit.Test;

import eu.ginere.jdbc.mysql.MySQLDataBase;
import eu.ginere.jdbc.mysql.MySQLDatabaseUtils;
import eu.ginere.jdbc.mysql.backend.BackendManager;
import eu.ginere.jdbc.mysql.dao.test.AbstractSQLDAOTest;
import eu.ginere.jdbc.mysql.dao.util.DBProperties;
import eu.ginere.jdbc.mysql.dao.util.DBPropertiesDAO;


public class DBPropertiesAutoTest extends AbstractSQLDAOTest {
	
	public DBPropertiesAutoTest(){
		super(DBPropertiesDAO.DAO,false);
	}
	
	protected void setDataSource() throws Exception {
		String filePropertiesName="conf/jdbc.properties";
		MySQLDatabaseUtils.initDatasource(filePropertiesName);
		MySQLDataBase.DEFAULT_DATABASE.createAndUseDatabase("junit_test");
		
		BackendManager.init();
	}
	
	@Test
	public void testInsertAndQuery() throws Exception {
		try {
			setDataSource();

			long elementNumber=DBPropertiesDAO.DAO.getBackendElementNumber();
			log.info("elementNumber:"+elementNumber);
			
			String value="test";
			String value2;
			String name="test";
			
			if (DBProperties.exists(DBPropertiesAutoTest.class,name)){
				DBProperties.delete(DBPropertiesAutoTest.class,name);
			}
			// insert
			DBProperties.setStringValue(DBPropertiesAutoTest.class,name,value);
			value2=DBProperties.getStringValue(DBPropertiesAutoTest.class, name, null);
			
			assertEquals(value, value2);
			
			// update
			DBProperties.setStringValue(DBPropertiesAutoTest.class, name, "test2");
			value2=DBProperties.getStringValue(DBPropertiesAutoTest.class, name, null);
			assertEquals("test2", value2);
			
			// false
			DBProperties.setStringValue(DBPropertiesAutoTest.class, name, "test3");
			value2=DBProperties.getStringValue(DBPropertiesAutoTest.class, name, null);
			assertFalse( "test2".equals(value2) );
			
			
			// delete
			
			DBProperties.delete(DBPropertiesAutoTest.class,name);
			

		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

}
