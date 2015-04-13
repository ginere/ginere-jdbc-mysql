package avem.jdbc.backend;

import java.util.Vector;

import org.apache.log4j.Logger;

import avem.common.util.dao.DaoManagerException;
import avem.jdbc.dao.AbstractSQLDAO;

public class BackendManager {
	static final Logger log = Logger.getLogger(AbstractSQLDAO.class);
	private static final int NO_EXISTS_BACKEND_VERSION = 0;

	public final static Vector <BackEndInterface>list=new Vector<BackEndInterface>();
	
	public static void subscrive(BackEndInterface backendInterface){
		list.add(backendInterface);
	}

	public static void init() {
		boolean ok=BackendDAO.DAO.isBackendOk();
		
		if (!ok){
			log.warn("The Backend stauff is not actualizad, code version is :"+BackendDAO.DAO.getCodeVersion()+
					 " installed version is:"+BackendDAO.DAO.getInstalledVersion()+". UPDATING  ....");
			try {
				BackendDAO.DAO.createorUpdateBackEnd();
			} catch (DaoManagerException e) {
				log.fatal("Error while updating backend");
			}			
		}
	}
	/**
	 * This returns the version of the Descriptor code. This version will be 
	 * used to compare width the backend version and to make the upgrade if 
	 * necessary
	 * @throws DaoManagerException 
	 */
	public int getBackendVersion(Class clazz) throws DaoManagerException{
		String id=clazz.getName();

		if (BackendDAO.DAO.exists(id)){
			BackendInfo info=BackendDAO.DAO.get(id);

			return info.getVersion();
		} else {
			return -1;
		}
	}


	public static void setCurrentVersion(Class clazz,int version) throws DaoManagerException {
		String id=clazz.getName();

		if (BackendDAO.DAO.exists(id)){
			BackendInfo info=BackendDAO.DAO.get(id);

			info.setVersion(version);
			BackendDAO.DAO.update(info);
		} else {
			BackendInfo info=new BackendInfo(id, version);
			BackendDAO.DAO.insert(info);
		}	
	}


	public static BackendInfo getInfo(Class clazz) {
		String id=clazz.getName();

		try {
			if (BackendDAO.DAO.exists(id)){
				return BackendDAO.DAO.get(id);
			} else {
				return new BackendInfo(id, NO_EXISTS_BACKEND_VERSION);
			}
		}catch (DaoManagerException e) {
			return new BackendInfo(id, NO_EXISTS_BACKEND_VERSION);
		}
	}


	public static void delete(Class clazz) {
		String id=clazz.getName();

		try {
			if (BackendDAO.DAO.exists(id)){
				BackendDAO.DAO.delete(id);
			} 
		}catch (DaoManagerException e) {
			log.error("Class:'"+clazz+"'",e);
		}
	}

	
}
