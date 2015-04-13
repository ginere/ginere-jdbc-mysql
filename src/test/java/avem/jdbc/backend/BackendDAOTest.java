package avem.jdbc.backend;

import java.util.List;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.junit.Test;

import avem.jdbc.JdbcManager;
import avem.jdbc.dao.AbstractKeyObjectSQLDAO;
import avem.jdbc.dao.AbstractSQLDAO;

public class BackendDAOTest extends TestCase{
	static final Logger log = Logger.getLogger(BackendDAOTest.class);


	private static void setDataSource() throws Exception {
		DataSource dataSource = JdbcManager.createMySQLDataSourceFromPropertiesFile("/etc/cgps/jdbc.properties");
		
		JdbcManager.setDataSource(dataSource);
	}

	@Test
	static public void testBackEnd() throws Exception {
		try {
			setDataSource();
			AbstractSQLDAO DAO=BackendDAO.DAO;
				
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
	static public void testKeyBackEnd() throws Exception {		
		try {
			setDataSource();
			AbstractKeyObjectSQLDAO DAO=BackendDAO.DAO;
				
			List listAll=DAO.getAll();
			List<String> listAllIds=DAO.getAllIds();
			long elementNumber=DAO.getBackendElementNumber();
			
			assertEquals(elementNumber, listAll.size());
			assertEquals(elementNumber, listAllIds.size());
			
			if (listAllIds.size() >0 ){
				String id=listAllIds.get(0);
				
				assertTrue(DAO.exists(id));
				
				Object obj=DAO.get(id);
				
				assertTrue(obj!=null);
			}
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}
}
