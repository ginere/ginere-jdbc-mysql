/**
 * Copyright: Angel-Ventura Mendo Gomez
 *	      ventura@free.fr
 *
 * $Id: BackendInfo.java,v 1.1 2005/12/30 12:48:21 ventura Exp $
 */
package eu.ginere.jdbc.mysql.backend;

import java.util.Date;

import eu.ginere.base.util.dao.cache.impl.AbstractKeyCacheObject;

/**
 *
 * @author Angel Mendo
 * @version $Revision: 1.1 $
 */
public class BackendInfo extends AbstractKeyCacheObject {

	private int version;
	
	public BackendInfo(String id,
					   int version) {
		
		super(id,System.currentTimeMillis());
		
		this.version=version;
	}


	public BackendInfo(String id,
					   int version,
					   long lastUpdate) {
		
		super(id,lastUpdate);
		
		this.version=version;
	}

	
	/**
	 * @return
	 */
	public Date getLastChange() {
		return new Date(lastUpdate());
	}

	/**
	 * @return
	 */
	public int getVersion() {
		return version;
	}


	void setVersion(int version2) {
		this.version=version2;
	}
}
