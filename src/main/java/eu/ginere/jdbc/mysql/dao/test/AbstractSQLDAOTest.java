package eu.ginere.jdbc.mysql.dao.test;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import eu.ginere.base.util.test.TestResult;
import eu.ginere.jdbc.mysql.MySQLDataBase;
import eu.ginere.jdbc.mysql.MySQLDatabaseUtils;
import eu.ginere.jdbc.mysql.backend.BackendManager;
import eu.ginere.jdbc.mysql.dao.AbstractDAO;

public abstract class AbstractSQLDAOTest extends TestCase {
	public static final Logger log = Logger.getLogger(AbstractSQLDAOTest.class);

	protected final AbstractDAO DAO;
	protected final boolean removeBackEnd;


	protected AbstractSQLDAOTest(AbstractDAO DAO){
		this.DAO=DAO;
		this.removeBackEnd=false;
	}


	protected AbstractSQLDAOTest(AbstractDAO DAO,boolean removeBackEnd){
		this.DAO=DAO;
		this.removeBackEnd=removeBackEnd;
	}
	
	public void innerTestBackEnd() throws Exception {
		try {
			setDataSource();
		
			if (removeBackEnd /* && DAO.getInstalledVersion() >0*/ ){ // Los backends que no tienen queries tienen version del codigo 0
				try {
					log.info("Borrando tablas ...");
					DAO.deleteBackEnd();
					BackendManager.delete(DAO.getClass());

					log.info("Creando tablas");
					DAO.createorUpdateBackEnd();
				}catch (Exception e) {
					log.info("While removing backend",e);
				}
			}
			

			int codeVersion=DAO.getCodeVersion();
			log.info("DAO:'"+DAO.getClass()+"' codeVersion:"+codeVersion);

			int installedVersion=DAO.getInstalledVersion();
			log.info("DAO:'"+DAO.getClass()+"' installedVersion:"+installedVersion);

			boolean ok=DAO.isBackendOk();
			log.info("DAO:'"+DAO.getClass()+"' isBackendOk:"+ok);
			
	
			if (!ok){
				DAO.createorUpdateBackEnd();
				log.info("updates: OK");
			} else {
				log.info("is updated.");

			}

			long elementNumber=DAO.getBackendElementNumber();
			log.info("elementNumber:"+elementNumber);
			
			TestResult test=DAO.test();
			
			if (!test.isOK()){
				log.error("Test not OK:"+test);	
			}
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

	protected String getFilePropertiesName() throws Exception {
		return "conf/jdbc.properties";
	}
	
	protected void setDataSource() throws Exception {
		String filePropertiesName=getFilePropertiesName();
		DataSource dataSource = MySQLDatabaseUtils.createMySQLDataSourceFromPropertiesFile(filePropertiesName);
		
//		MySQLDatabaseUtils.setDataSource(dataSource);
		
		MySQLDataBase.initDatasource(filePropertiesName,dataSource);
		BackendManager.init();
	}

}
