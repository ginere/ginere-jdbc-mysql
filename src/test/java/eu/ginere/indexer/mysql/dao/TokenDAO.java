//package eu.ginere.indexer.mysql.dao;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.log4j.Logger;
//
//import eu.ginere.base.util.dao.DaoManagerException;
//import eu.ginere.base.util.test.TestResult;
//import eu.ginere.jdbc.mysql.MySQLDataBase;
//import eu.ginere.jdbc.mysql.dao.AbstractDAO;
//
//public class TokenDAO extends AbstractDAO implements TokenDAOInterface{
//
//	static final Logger log = Logger.getLogger(TokenDAO.class);
//	
//	static final private String TABLE_NAME= "TOKENS_COUNT";
//	static final private String[] COLUMNS=new String[] {
//		"TOKEN", 
//		"TYPE",
//		"NUMBER",
//		"LAST_UPDATE"
//	};
//
//	static final private String CREATE_QUERY_ARRAY[][]=new String[][]{
//		{ // V1
//			"CREATE TABLE "+TABLE_NAME+" ("
//			+ "		TOKEN varchar(2000) NOT NULL,"
//			+ "		TYPE varchar(1000) NOT NULL,"
//			+ "		NUMBER INTEGER NOT NULL,"
//			+ "		LAST_UPDATE TIMESTAMP default SYSDATE NOT NULL"
//			+ ");",
//		},{
//			// V2
//			"CREATE INDEX TOKEN_INDEX ON "+TABLE_NAME+" (TOKEN,TYPE);"
//		}
//	};
//
//	public static final TokenDAO DAO=new TokenDAO();
//
//	private TokenDAO(){
//		super(TABLE_NAME,CREATE_QUERY_ARRAY);		
//	}
//
//	private static final String INSERT = "insert into "+TABLE_NAME+
//		" (TOKEN,TYPE,NUMBER) values (?,?,?)";
//
//	public void insert(TokenElement indx) throws DaoManagerException{
//		insert(indx.getToken(),
//			   indx.getType(),
//			   indx.getCount());
//	}
//
//	public void insert(String token,String type,int number) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=INSERT;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//
//			setString(pstm, 1, token, query);
//			setString(pstm, 2, type, query);
//			setInt(pstm, 3, number, query);
//	
//			executeUpdate(pstm, query);
//	
//		} catch (DaoManagerException e) {
//			String error = "token:'"+token
//				+ "' type:'"+type
//				+ "' number:'"+number+"'";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//	private static final String DELETE = "delete from "+TABLE_NAME+
//	" where TOKEN=? and TYPE=?";
//
//	public void delete(String token, String type) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=DELETE;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//			setString(pstm, 1, token, query);
//			setString(pstm, 2, type, query);
//	
//			long number=executeUpdate(pstm, query);
//	
//			if (log.isDebugEnabled()){
//				log.debug("Rows modified:"+number);
//			}
//		} catch (DaoManagerException e) {
//			String error = " token:'"+type
//				+ "' type:'"+type+"'";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//
//    //	private static final String AUTO_COMPLETE = "SELECT token,number FROM "+TABLE_NAME+" where type=? and token like ? order by LENGTH(token) desc,number desc LIMIT ?";
//	private static final String AUTO_COMPLETE = "SELECT token,number FROM "+TABLE_NAME+" where type=? and token like ? order by number desc LIMIT ?";
//
//	public List<String> getAutoComplete(String lastToken, 
//                                        String type,
//                                        int numberAutoComplete) throws DaoManagerException{
//		if (type == null || "".equals(type)){
//			return getAutoComplete(lastToken,numberAutoComplete);
//		} else {
//			Connection connection = getConnection();
//			String query=AUTO_COMPLETE;
//			
//			try {
//				PreparedStatement pstm = getPrepareStatement(connection,query);
//				
//				setString(pstm, 1, type, query);
////				setString(pstm, 2, '%'+lastToken+'%', query);
//				setString(pstm, 2, lastToken+'%', query);
//				setInt(pstm, 3, numberAutoComplete, query);
//				
//				return getStringList(pstm, query);
//				
//			} catch (DaoManagerException e) {
//				String error = " lastToken:'"+lastToken+"'";
//				
//				log.error(error, e);
//				throw new DaoManagerException(error, e);
//			} finally {
//				closeConnection(connection);
//			}
//		}
//	}
//
//    //	private static final String AUTO_COMPLETE_NO_TYPE = "SELECT token,number FROM "+TABLE_NAME+" where token like ? order by LENGTH(token) desc, number desc LIMIT ?";
//	private static final String AUTO_COMPLETE_NO_TYPE = "SELECT token,number FROM "+TABLE_NAME+" where token like ? order by number desc LIMIT ?";
//
//	public List<String> getAutoComplete(String lastToken, 
//                                        int numberAutoComplete) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=AUTO_COMPLETE_NO_TYPE;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
////			setString(pstm, 1, '%'+lastToken+'%', query);
//			setString(pstm, 1, lastToken+'%', query);
//			setInt(pstm, 2, numberAutoComplete, query);
//			
//			return getStringList(pstm, query);
//			
//		} catch (DaoManagerException e) {
//			String error = " lastToken:'"+lastToken+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//	
//	private static final String SORT_TOKEN_START = "select token from "+TABLE_NAME+" where type = ? and ( TOKEN=? ";
//	private static final String SORT_TOKEN_START_NO_TYPE = "select token,sum(number) as number from "+TABLE_NAME+" where ( TOKEN=? ";
//	private static final String SORT_TOKEN_MIDDLE = " or TOKEN=? ";
//	
//	private static final String SORT_TOKEN_STOP = " ) order by number desc;";
//	private static final String SORT_TOKEN_STOP_NON_TYPE = " ) group by token order by number desc;";
//
//	public Collection<String> sort(HashSet<String> tokens) throws DaoManagerException{
//		
//		if (tokens == null || tokens.size()<=1){
//			return tokens;
//		}
//		StringBuilder buffer=new StringBuilder();
//		
//		for (int i=0;i<tokens.size();i++){
//			if (i == 0) {
//				buffer.append(SORT_TOKEN_START_NO_TYPE);				
//			} else {
//				buffer.append(SORT_TOKEN_MIDDLE);
//			}
//		}
//		
//		buffer.append(SORT_TOKEN_STOP_NON_TYPE);
//		
//		Connection connection = getConnection();
//		String query=buffer.toString();
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//			int i=1;
//			for (String token:tokens){
//				setString(pstm, i++, token, query);
//			}
//			
//			return getStringList(pstm, query);
//			
//		} catch (DaoManagerException e) {
//			String error = " tokens:'"+tokens+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//
//	@Override
//	public Collection<String> sort(HashSet<String> tokens,
//                                   String type) throws DaoManagerException{
//		if (tokens == null || tokens.size()<=1){
//			return tokens;
//		}
//
//		if (type == null || "".equals(type)){
//			return sort(tokens);
//		} 
//
//		StringBuilder buffer=new StringBuilder();
//		
//		for (int i=0;i<tokens.size();i++){
//			if (i == 0) {
//				buffer.append(SORT_TOKEN_START);				
//			} else {
//				buffer.append(SORT_TOKEN_MIDDLE);
//			}
//		}
//		
//		buffer.append(SORT_TOKEN_STOP);
//		
//		Connection connection = getConnection();
//		String query=buffer.toString();
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//			int i=1;
//			setString(pstm, i++, type, query);
//			for (String token:tokens){
//				setString(pstm, i++, token, query);
//			}
//			
//			return getStringList(pstm, query);
//			
//		} catch (DaoManagerException e) {
//			String error = " tokens:'"+tokens+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//
//	public void updateOrInsert(String token, String type,int number) throws DaoManagerException {
//		if (exists(token,type)){
//			updateTokenValue(token,type,number);
//		} else {
//			insert(token, type, number);
//		}
//		
//	}
//
//	private static final String UPDATE_INCREMENT_TOKEN = "update TOKENS_COUNT SET number=? where TOKEN=? and TYPE=?";
//
//	public long updateTokenValue(String token, String type,int number) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=UPDATE_INCREMENT_TOKEN;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//			setInt(pstm, 1, number, query);
//			setString(pstm, 2, token, query);
//			setString(pstm, 3, type, query);
//			
//			return executeUpdate(pstm, query);
//			
//		} catch (DaoManagerException e) {
//			String error = " lastToken:'"+token+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//	
//
//	
//	private static final String EXISTS_FOR_TOKEN_TYPE = "select NUMBER from " + TABLE_NAME
//	+ " where TOKEN=? AND TYPE=? LIMIT 1";
//	
//	private boolean exists(String token, String type) throws DaoManagerException {
//		String query=EXISTS_FOR_TOKEN_TYPE;
//		Connection connection = getConnection();
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//	
//			setString(pstm, 1, token, query);
//			setString(pstm, 2, type, query);
//	
//			ResultSet rset = executeQuery(pstm,query);
//			
//			return rset.next();
//		} catch (SQLException e) {
//			String error = "token:'"+token+"' type:"+type;
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//	
//		} catch (DaoManagerException e) {
//			String error = "token:'"+token+"' type:"+type;
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//
//	private static final String DELETE_FOR_TYPE = "delete from "+TABLE_NAME+" where type=?";
//	
//	public void delete(String type) throws DaoManagerException {
//		Connection connection = getConnection();
//		String query=DELETE_FOR_TYPE;
//		
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//						
//			setString(pstm, 1, type, query);
//			
//	
//			executeUpdate(pstm, query);
//		} catch (DaoManagerException e) {
//			String error = " type:'"+type;
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//		
//	}
//
//	private static final String GET_COUNT = "SELECT number FROM "+TABLE_NAME+" where type=? and token=?;";
//
//	public int getCount(String token, String type) throws DaoManagerException{
//		if (type == null || "".equals(type)){
//			return getCount(token);
//		} else {
//			Connection connection = getConnection();
//			String query=GET_COUNT;
//			
//			try {
//				PreparedStatement pstm = getPrepareStatement(connection,query);
//				
//				setString(pstm, 1, type, query);
//				setString(pstm, 2, token, query);
//				
//				return getIntFromQuery(pstm, query, 0);
//				
//			} catch (DaoManagerException e) {
//				String error = " token:'"+token+"'";
//				
//				log.error(error, e);
//				throw new DaoManagerException(error, e);
//			} finally {
//				closeConnection(connection);
//			}
//		}
//	}
//	
//	///
//	
//	private static final String GET_COUNT_NO_TYPE = "SELECT number FROM "+TABLE_NAME+" where token=?;";
//
//	public int getCount(String token) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=GET_COUNT_NO_TYPE;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//				
//			setString(pstm, 1, token, query);
//			
//			return getIntFromQuery(pstm, query, 0);
//			
//		} catch (DaoManagerException e) {
//			String error = " token:'"+token+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//	///
//
//	private static final String GET_COUNT_VAR = "SELECT min(number) FROM "+TABLE_NAME+" where type=? and ";
//
//	public int getCount(HashSet<String> tokens, String type) throws DaoManagerException {
//		if (type == null || "".equals(type)){
//			return getCount(tokens);
//		} else {
//			Connection connection = getConnection();
//			StringBuilder buffer=new StringBuilder();
//
//			buffer.append(GET_COUNT_VAR);
//			for (int i=0;i<tokens.size();i++){
//				if (i != 0) {
//					buffer.append(" or ");			
//				}
//				buffer.append(" token=? ");
//			}
//			String query=buffer.toString();
//				
//			try {
//				PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//				int i=1;
//				setString(pstm, i++, type, query);
//				for (String token:tokens){
//					setString(pstm, i++, token, query);
//				}
//			
//				return getIntFromQuery(pstm, query, 0);
//			
//			} catch (DaoManagerException e) {
//				String error = " searchString:'"+StringUtils.join(tokens, ",")+"' type:"+type;
//			
//				log.error(error, e);
//				throw new DaoManagerException(error, e);
//			} finally {
//				closeConnection(connection);
//			}
//		}
//	}
//
//	///
//	
//	private static final String GET_COUNT_VAR_NO_TYPE = "SELECT min(number) FROM "+TABLE_NAME+" where  ";
//
//	public int getCount(HashSet<String> tokens) throws DaoManagerException {
//		Connection connection = getConnection();
//		StringBuilder buffer=new StringBuilder();
//
//		buffer.append(GET_COUNT_VAR_NO_TYPE);
//		for (int i=0;i<tokens.size();i++){
//			if (i != 0) {
//				buffer.append(" or ");			
//			}
//			buffer.append(" token=? ");
//			}
//		String query=buffer.toString();
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//				
//			int i=1;
//			for (String token:tokens){
//				setString(pstm, i++, token, query);
//			}
//			
//			return getIntFromQuery(pstm, query, 0);
//			
//		} catch (DaoManagerException e) {
//			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' no type.";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//	
//	///
//	private final static String ITERATE_TOKEN_TABLE_ON_TYPE="select TOKEN from  "+TABLE_NAME+" where type=? AND TOKEN in (SELECT DISTINCT TOKEN FROM "+IndexerDAO.TABLE_NAME+")";
//
//	private void update(String type) throws DaoManagerException {
//		
//		Connection connection = getConnection();
//		String query=ITERATE_TOKEN_TABLE_ON_TYPE;
//		
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			setString(pstm, 1, type, query);
//			try {
//				
//				ResultSet rset = executeQuery(pstm,query);
//				try {
//					while (rset.next()){
//						String token=rset.getString(1);
//						int number=IndexerDAO.DAO.getNumber(type,token,-1);
//						
//						if (number>0){
//							update(type,token,number);
//						} else {
//							log.warn("No index values for token:"+token+" type:"+type);
//							delete(type,token);
//						}
//					}
//					return ;
//				}catch (SQLException e){
//					String error="Query" + query;
//					throw new DaoManagerException(error,e);
//				}finally{
//					close(rset);
//				}
//			}finally{
//				close(pstm);
//			}			
//		} catch (DaoManagerException e) {
//			String error = "type:'"+type+"' ";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//	
//	private final static String ITERATE_INDEXER_TOKEN_NOT_IN_TOKEN_COUNT="select DISTINCT TOKEN from  "+IndexerDAO.TABLE_NAME+" where type=? AND TOKEN NOT in (SELECT DISTINCT TOKEN FROM "+TABLE_NAME+")";
//	
//	private void insert(String type) throws DaoManagerException {
//		
//		Connection connection = getConnection();
//		String query=ITERATE_INDEXER_TOKEN_NOT_IN_TOKEN_COUNT;
//		
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			setString(pstm, 1, type, query);
//			try {
//				
//				ResultSet rset = executeQuery(pstm,query);
//				try {
//					while (rset.next()){
//						String token=rset.getString(1);
//						int number=IndexerDAO.DAO.getNumber(type,token,-1);
//						try {
//							if (number>0){
//								insert(token,type,number);
//							} else {
//								log.warn("No index values for token:"+token+" type:"+type);
//								// nothing to do
//							}
//						}catch(DaoManagerException e){
//							throw new DaoManagerException("type:'"+type+"' toke:'"+token+"' number:"+number+"'", e);
//						}
//					}
//					return ;
//				}catch (SQLException e){
//					String error="Query" + query;
//					throw new DaoManagerException(error,e);
//				}finally{
//					close(rset);
//				}
//			}finally{
//				close(pstm);
//			}			
//		} catch (DaoManagerException e) {
//			String error = "type:'"+type+"' ";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//	private final static String UPDATE_TYPE_TOKENS="update "+TABLE_NAME+" set NUMBER=?, LAST_UPDATE=SYSDATE where type=? AND TOKEN =? ";
//	
//	/**
//	 * Delete all tokens that are not in the indxer table
//	 * @param type
//	 * @throws DaoManagerException
//	 */
//	private void update(String type,
//                        String token,
//                        int count) throws DaoManagerException {
//		
//		Connection connection = getConnection();
//		String query=UPDATE_TYPE_TOKENS;
//		
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			int i=1;
//			setInt(pstm, i++, count, query);
//			setString(pstm, i++, type, query);
//			setString(pstm, i++, token, query);
//			
//			try {
//				executeQuery(pstm, query);
//			}finally{
//				close(pstm);
//			}			
//		} catch (DaoManagerException e) {
//			String error = "type:'"+type+"' ";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//
//	@Override
//	public void updateTokenCount(String type) throws DaoManagerException {
//		log.info("Deleting ...");
//		delete(type);
//		log.info("Update ...");
//		update(type);
//		log.info("Insert ...");
//		insert(type);
//	}
//
//
//	private static final String GET_TOKEN_NUMBER = "select count(*) from "+TABLE_NAME+" ";
//	
//	public int getNumber(int defaultValue) throws DaoManagerException {
//		Connection connection = getConnection();
//		String query=GET_TOKEN_NUMBER;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				return getIntFromQuery(pstm,query, defaultValue);	
//			} finally {
//				close(pstm);
//			}			
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//	@Override
//	public TestResult test() {
//		TestResult ret=new TestResult(getClass());
//		ret.add(super.test());
//
//		if (ret.isOK()){
//			try {
//				getNumber(0);
//			} catch (DaoManagerException e) {
//				ret.addError("Getting the number of elements", e);
//			}
//		}
//		return ret;
//	}
//	
//	public void init(MySQLDataBase dataBase) throws DaoManagerException {
//		setDataBase(dataBase);
//	
//		TestResult ret=test();
//		
//		if (ret.isOK()){
//			return ;
//		} else {
//			// create an test if everything is ok
//			createTable();
//			ret=test();
//			if (ret.isOK()){
//				return;
//			} else {
//				throw new DaoManagerException("Error creating tables..."+ret);
//			}
//		}
//		
//	}
//}
