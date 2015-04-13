package avem.jdbc.backend;

import avem.common.util.dao.DaoManagerException;

public interface BackEndInterface {

	/**
	 * Test if the backend is working fine
	 *
	 * @return Return one humah readeable string 
	 */
	public boolean isBackendOk()throws DaoManagerException;
	
	/**
	 * Permorm the creation or updating stuffs
	 * 
	 * @return Return one humah readeable string 
	 */
	public void createorUpdateBackEnd()throws DaoManagerException;

	/**
	 * Returns the backend Element Number
	 * 
	 * @return
	 */
	public long getBackendElementNumber();

	/**
	 * The version of the code
	 * @return
	 */
	public int getCodeVersion();
	
	
	/**
	 * The version of the hardware backend
	 * @return
	 */
	public int getInstalledVersion();

}
