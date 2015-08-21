//package eu.ginere.indexer.mysql.dao;
//
//import java.io.Serializable;
//import java.util.Date;
//
//import org.apache.commons.lang.builder.ToStringBuilder;
//import org.apache.commons.lang.builder.ToStringStyle;
//
///**
// * This represents a token of an object to be stored into the database to search.
// * 
// * @author ginere
// *
// */
//public class IndexerElement implements Serializable {
//
//	/**
//	 * Serial Id
//	 */
//	private static final long serialVersionUID = "$Version$".hashCode();
//	
//	/**
//	 * The token
//	 */
//	public final String token;
//	
//	/**
//	 * For type of the object containing the token
//	 */
//	public final String type;
//	
//	/**
//	 * Unic ID of the token 
//	 */
//	public final String id;
//	
//	/**
//	 * Last time this token was update. Usefull to update already inxed objects.
//	 */
//	public final Date lastModification;
//
//	public IndexerElement(String tokens, 
//				  String type, 
//				  String id) {
//		this.token=tokens;
//		this.type=type;
//		this.id=id;
//		this.lastModification=new Date();
//	}
//	
//	public IndexerElement(String tokens, 
//			  String type, 
//			  String id,
//			  Date lastModification) {
//	this.token=tokens;
//	this.type=type;
//	this.id=id;
//	this.lastModification=lastModification;
//}
//
//	public String getToken(){
//		return token;
//	}
//
//	public String getType() {
//		return type;
//	}
//
//	public String getId() {
//		return id;
//	}
//	
//	public String toString(){
//		return ToStringBuilder.reflectionToString(this,ToStringStyle.SIMPLE_STYLE);		
//	}
//
//}
//
//
