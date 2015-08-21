package eu.ginere.jdbc.mysql.backend;

import javax.sql.DataSource;

import org.junit.Test;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.jdbc.mysql.MySQLDataBase;
import eu.ginere.jdbc.mysql.MySQLDatabaseUtils;
import eu.ginere.jdbc.mysql.backend.BackendDAO;
import eu.ginere.jdbc.mysql.backend.BackendInfo;
import eu.ginere.jdbc.mysql.dao.test.AbstractKeyDaoTest;

public class BackendDAOAutoTest extends AbstractKeyDaoTest<BackendInfo>{
	
	public BackendDAOAutoTest(){
		super(BackendDAO.DAO,false);
	}

	@Override
	protected BackendInfo getTestObj() {
		return new BackendInfo("testId", 1);
	}
	
	protected void cleanTestBeforeStart() throws DaoManagerException{
		// do nothing
	}
	
	protected void setDataSource() throws Exception {
		String filePropertiesName="conf/jdbc.properties";
		MySQLDatabaseUtils.initDatasource(filePropertiesName);
		MySQLDataBase.DEFAULT_DATABASE.createAndUseDatabase("junit_test");
		
		BackendManager.init();
	}

	
	@Test
	public void testMain() throws Exception {	
		executeText();
	}
}
