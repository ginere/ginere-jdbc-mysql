//package eu.ginere.indexer.mysql.dao;
//
//import org.junit.Test;
//
//import eu.ginere.jdbc.mysql.MySQLDataBase;
//import eu.ginere.jdbc.mysql.MySQLDatabaseUtils;
//import eu.ginere.jdbc.mysql.backend.BackendManager;
//import eu.ginere.jdbc.mysql.dao.test.AbstractSQLDAOTest;
//
//
//public class IndexerDAOAutoTest extends AbstractSQLDAOTest {
//	
//	public IndexerDAOAutoTest(){
//		super(IndexerDAO.DAO,false);
//	}
//	
//	protected void setDataSource() throws Exception {
//		String filePropertiesName="conf/jdbc.properties";
//		MySQLDatabaseUtils.initDatasource(filePropertiesName);
//		MySQLDataBase.DEFAULT_DATABASE.createAndUseDatabase("junit_test");
//		
//		BackendManager.init();
//	}
//	
//	@Test
//	public void testMain() throws Exception {
//		try {
//			setDataSource();
//
//			innerTestBackEnd();
//
//		} catch (Exception e) {
//			log.error("", e);
//			throw e;
//		}
//	}
//
//}
