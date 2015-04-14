package eu.ginere.jdbc.mysql.dao.util;

import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;


public class DBProperties  {
	static final Logger log = Logger.getLogger(DBProperties.class);
	
	public static int getIntValue(Class c, String propertyName, int defaultValue) {
        return DBPropertiesDAO.DAO.getIntValue(c, propertyName, defaultValue);
	}

	public static void setIntValue(Class c, String propertyName, int value) throws DaoManagerException {
		setStringValue(c,propertyName,Integer.toString(value));
	}

	public static boolean getBooleanValue(Class section, 
                                   String propertyName,
                                   boolean defaultValue) {
        return DBPropertiesDAO.DAO.getBooleanValue(section,propertyName,defaultValue);
	}

	public static void setBooleanValue(Class c, String propertyName, boolean value) throws DaoManagerException {
		setStringValue(c,propertyName,((value)?"true":"false"));
	}

	public static String getStringValue(Class c, String propertyName,String defaultValue) {
        return DBPropertiesDAO.DAO.getStringValue(c,propertyName,defaultValue);
	}
		
	public static void setStringValue(Class c, String propertyName,String value) throws DaoManagerException {
        DBPropertiesDAO.DAO.setStringValue(c,propertyName,value);
	}

    /*
	public String get(Class c,String propertyName,String defaultvalue) throws DaoManagerException {		
        return DBPropertiesDAO.DAO.get(c,propertyName.defaultvalue);
	}
    */
	public static boolean exists(Class c,String propertyName) throws DaoManagerException {		
        return DBPropertiesDAO.DAO.exists(c,propertyName);
	}
    /*
	public void insert(Class c,String propertyName,String value) throws DaoManagerException {		
        DBPropertiesDAO.DAO.insert(c,propertyName,value);
	}

	public void update(Class c,String propertyName,String value) throws DaoManagerException {		
        DBPropertiesDAO.DAO.update(c,propertyName,value);
	}
    */

	public static void delete(Class c,String propertyName) throws DaoManagerException {		
        DBPropertiesDAO.DAO.delete(c,propertyName);
	}	
}

