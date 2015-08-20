package com.ikanow.aleph2.security.service;



import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SaltedAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;
import org.apache.shiro.codec.Base64;
import org.apache.shiro.codec.Hex;
import org.apache.shiro.crypto.hash.Hash;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.StringUtils;

public class IkanowV1CredentialsMatcher extends SimpleCredentialsMatcher {
	private static final Logger logger = LogManager.getLogger(IkanowV1CredentialsMatcher.class);

	
    /**
     * This implementation first hashes the {@code token}'s credentials, potentially using a
     * {@code salt} if the {@code info} argument is a
     * {@link org.apache.shiro.authc.SaltedAuthenticationInfo SaltedAuthenticationInfo}.  It then compares the hash
     * against the {@code AuthenticationInfo}'s
     * {@link #getCredentials(org.apache.shiro.authc.AuthenticationInfo) already-hashed credentials}.  This method
     * returns {@code true} if those two values are {@link #equals(Object, Object) equal}, {@code false} otherwise.
     *
     * @param token the {@code AuthenticationToken} submitted during the authentication attempt.
     * @param info  the {@code AuthenticationInfo} stored in the system matching the token principal
     * @return {@code true} if the provided token credentials hash match to the stored account credentials hash,
     *         {@code false} otherwise
     * @since 1.1
     */
    @Override
    public boolean doCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) { 
        String plainPassword = new String(((UsernamePasswordToken)token).getPassword());
        String accountCredentials = (String)getCredentials(info);
        try {
			return checkPassword(plainPassword, accountCredentials);
		} catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return false;
    }
	/**
	 *  Encrypt the password
	 * @throws NoSuchAlgorithmException 
	 * @throws UnsupportedEncodingException 
	 */
	public static String encrypt(String password) throws NoSuchAlgorithmException, UnsupportedEncodingException 
	{	
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(password.getBytes("UTF-8"));		
		return Base64.encodeToString(md.digest());		
	}
	/**
	 *  Check the password
	 * @throws UnsupportedEncodingException 
	 * @throws NoSuchAlgorithmException 
	 */
	public static boolean checkPassword(String plainPassword, String encryptedPassword) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		return encryptedPassword.equals(encrypt(plainPassword));
		//return encryptor.checkpw(plainPassword, encryptedPassword);
	}	
	
	/**
	 * Checks if a user is in authentication DB
	 * and returns their userid if successful.
	 * Returns null otherwise.
	 * 
	 * @param username
	 * @param userEncryptPword
	 * @return
	 */
/*	public static AuthenticationPojo validateUser(String username, String userEncryptPword)
	{
		return validateUser(username, userEncryptPword, true);
	}
	*/
/*	public static AuthenticationPojo validateUser(String username, String userPword, boolean bPasswdEncrypted)
	{
		try
		{
			//Get user auth on username
			BasicDBObject query = new BasicDBObject();
			query.put("username", username);
			DBObject dbo = DbManager.getSocial().getAuthentication().findOne(query);
			if (dbo != null )
			{			
				//	check if pwords match
				AuthenticationPojo ap = AuthenticationPojo.fromDb(dbo, AuthenticationPojo.class);
				//only active accts can login (not pending or disabled)
				if ( (ap.getAccountStatus() == null) || ( ap.getAccountStatus() == AccountStatus.ACTIVE ) )
				{
					//(legacy users have accountStatus==null)
					
					if ( ap.getPassword().equals(userPword))
					{
						return ap;
					}
					else if (!bPasswdEncrypted) 
					{
						if ( ap.getPassword().equals(encrypt(userPword)))
						{
							return ap;
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			// If an exception occurs log the error
			logger.error("Messaging Exception Message: " + e.getMessage(), e);
		}
		
		return null;
	}
	*/
/*	public static String md5checksum(String toHash)
	{
		try
		{
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(toHash.getBytes(Charset.forName("UTF8")));
			byte[] digest = m.digest();
			return new String(Hex.encode(digest));
		}
		catch (Exception ex)
		{
			return toHash;
		}		
	}
*/

}