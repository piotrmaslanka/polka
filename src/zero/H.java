package zero;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The principal hashing function
 *
 */
public class H {
	
	/**
	 * Hashes the name.
	 * @param name Name to hash
	 * @param replica_no Number of replica, counted from 0
	 * @return Hash value
	 */
	public static long hash(String name, int replica_no) {
		return H.h_hash(name) + replica_no * 2305843009213693952L;
	}
	
	private static long h_hash(String name) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(name.getBytes(Charset.forName("UTF-8")));
			byte[] digest = md.digest();
			
			ByteBuffer bb = ByteBuffer.allocate(8);
			bb.put(digest);
			bb.flip();
			return bb.getLong();
			
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("No SHA-1 on this platform!");
		}
		
		
	}
}
