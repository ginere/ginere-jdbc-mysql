package avem.jdbc.dao;


/**
 * @author ventura
 * Propiedades de un dao
 *
 */
public interface DAOInterface {

	/**
	 * DEvuelve el nombre de la tabla
	 * @return
	 */
	public String getTableName();

	/**
	 * Returuns all the columns Name
	 * @return
	 */
	public String[] getColumnsName();
	
}
