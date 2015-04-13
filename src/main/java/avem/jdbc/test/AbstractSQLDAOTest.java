package avem.jdbc.test;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import avem.jdbc.JdbcManager;
import avem.jdbc.backend.BackendManager;
import avem.jdbc.dao.AbstractSQLDAO;

public abstract class AbstractSQLDAOTest extends TestCase {
	public static final Logger log = Logger.getLogger(AbstractSQLDAOTest.class);

	protected final AbstractSQLDAO DAO;
	protected final boolean removeBackEnd;


	protected AbstractSQLDAOTest(AbstractSQLDAO DAO){
		this.DAO=DAO;
		this.removeBackEnd=false;
	}


	protected AbstractSQLDAOTest(AbstractSQLDAO DAO,boolean removeBackEnd){
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
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

	protected String getFilePropertiesName() throws Exception {
		return "/etc/cgps/jdbc.properties";
	}
	
	protected void setDataSource() throws Exception {
		String filePropertiesName=getFilePropertiesName();
		DataSource dataSource = JdbcManager.createMySQLDataSourceFromPropertiesFile(filePropertiesName);
		
		JdbcManager.setDataSource(dataSource);
		
		BackendManager.init();
	}

}
