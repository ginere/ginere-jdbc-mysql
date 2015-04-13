package avem.jdbc.dao.cache;

import org.junit.Test;

import eu.ginere.jdbc.mysql.dao.test.AbstractSQLDAOTest;
import eu.ginere.jdbc.mysql.dao.util.DBProperties;


public class DBPropertiesTest2 extends AbstractSQLDAOTest {
	
	public DBPropertiesTest2(){
		super(DBProperties.DAO,false);
	}
	
	@Test
	public void testInsertAndQuery() throws Exception {
		try {
			setDataSource();

			long elementNumber=DBProperties.DAO.getBackendElementNumber();
			log.info("elementNumber:"+elementNumber);
			
			String value="test";
			String value2;
			String name="test";
			
			if (DBProperties.exists(DBPropertiesTest2.class,name)){
				DBProperties.delete(DBPropertiesTest2.class,name);
			}
			// insert
			DBProperties.setStringValue(DBPropertiesTest2.class,name,value);
			value2=DBProperties.getStringValue(DBPropertiesTest2.class, name, null);
			
			assertEquals(value, value2);
			
			// update
			DBProperties.setStringValue(DBPropertiesTest2.class, name, "test2");
			value2=DBProperties.getStringValue(DBPropertiesTest2.class, name, null);
			assertEquals("test2", value2);
			
			// false
			DBProperties.setStringValue(DBPropertiesTest2.class, name, "test3");
			value2=DBProperties.getStringValue(DBPropertiesTest2.class, name, null);
			assertFalse( "test2".equals(value2) );
			
			
			// delete
			
			DBProperties.delete(DBPropertiesTest2.class,name);
			

		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

}
