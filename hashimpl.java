package heapfile;

/**
 * @author Banu
 *  Database Systems - HASH IMPLEMENTATION
 */

public interface hashimpl 
{
	public static final String HEAP_FNAME = "heap.";
	public static final String HASH_NAME = "hash.";
	public static final String ENCODING = "utf-8";
	
	//Heap
	public static final int RECORD_SIZE = 297;
	public static final int INT_SIZE = 4;
	public static final int RID_SIZE = INT_SIZE;
	public static final int REGISTER_NAME_SIZE = 14;
	public static final int BN_NAME_SIZE = 200;
	public static final int BN_NAME_OFFSET = RID_SIZE + REGISTER_NAME_SIZE;
	public static final int BN_STATUS_OFFSET = RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE;
	
	//Hash
	public static final int PAGE_SIZE = INT_SIZE;
	public static final int KEY_SIZE = INT_SIZE;
	public static final int EMPTY_HASH_INDICATOR = 1; //Indicates whether or not hash index row is empty
	public static final int HASH_INDEX_SIZE = EMPTY_HASH_INDICATOR + RID_SIZE + PAGE_SIZE + KEY_SIZE;
	public static final int BUCKETS = 553279; //Prime number
	public static final double EXPECTED_OCCUPANCY = 0.7;
	
   public void readArguments(String args[]);
   
	/**
	 *  Calculates bucketsize based on Number of buckets set and the Expected Occupancy
	 */
   public int calculateBucketSize();

	/**
	 *  Generates a hash key for a business name using Java's hashcode method
	 */
   public int getKey(byte[] bname);
	
   public int byteToInt(byte[] b);
   
	/**
	 *  Moves pointer to the first hash-offset pair in the bucket
	 */
   public int moveToBucket(int hashkey);
}
