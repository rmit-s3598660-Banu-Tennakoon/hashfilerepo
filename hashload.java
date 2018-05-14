package heapfile;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class creates a hashfile.
 * @param page size of the heapfile.
 * @return hash file built on the specified heapfile.
 */

public class hashload implements hashimpl{

	public static int BUCKET_SIZE;
	public File hashfile;
	public File heapfile;
	public static int hash_entries = 0;

	public static void main (String args[]) {
		
		hashload load = new hashload();
		
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();
		
		System.out.println("Time taken: " + (endTime-startTime) + "ms");
		getStatistics();
		
	}
	
	public void readArguments(String args[]) {
		try {
			readHeap(Integer.parseInt(args[0]));
		} catch (NumberFormatException e) {
		    System.err.println("Please enter the page size of the heap file.");
		}
	}
	
	/**
	 * This method is a modified version of the readHeap() method provided by RMIT as part of Assignment 1 
	 * heap file solution.
	 * It scans each business name from the heapfile and generates a hashkey. It then writes the hash key 
	 * and its rid-page-offset pair in a bucket corresponding to that hashkey.
	 */	
	
	public void readHeap(int pagesize) {
		heapfile = new File (HEAP_FNAME + pagesize);
		hashfile = new File (HASH_NAME + pagesize);
		
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		
		try {
			FileInputStream fis = new FileInputStream(heapfile);
			FileOutputStream fos = new FileOutputStream(hashfile);
			RandomAccessFile raf = new RandomAccessFile(hashfile, "rw"); 
			
			BUCKET_SIZE = calculateBucketSize();
			createBuckets(fos);
			System.out.println("Buckets initialised");
			fos.close();
			
			while (isNextPage) {
				byte[] bytePage = new byte[pagesize];
				byte[] bytePageNum = new byte[INT_SIZE];
				fis.read(bytePage, 0, pagesize);
				System.arraycopy(bytePage, bytePage.length-INT_SIZE, bytePageNum, 0, INT_SIZE);
				isNextRecord = true;
				
				while (isNextRecord) {
					byte[] byteRecord = new byte[RECORD_SIZE];
					byte[] byteRid = new byte[INT_SIZE];
					
					try {
					System.arraycopy(bytePage, recordLen, byteRecord, 0, RECORD_SIZE);
					System.arraycopy(byteRecord, 0, byteRid, 0, INT_SIZE);
					rid = byteToInt(byteRid);
					if (rid != recCount) {
						isNextRecord = false;
					} else {
						byte[] business_name = Arrays.copyOfRange(byteRecord, BN_NAME_OFFSET, BN_STATUS_OFFSET);
						byte[] key = ByteBuffer.allocate(INT_SIZE).putInt(getKey(business_name)).array();
						
						byte[] hash = new byte[RID_SIZE + PAGE_SIZE + KEY_SIZE];
						
						System.arraycopy(byteRid, 0, hash, 0, byteRid.length);
						System.arraycopy(bytePageNum, 0, hash, 4, bytePageNum.length);
						System.arraycopy(key, 0, hash, 8, key.length);
						
						insertKeyOffsetPair(hash, raf);
						recordLen += RECORD_SIZE;
					}
					recCount++;
					}	catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
				}
				if (ByteBuffer.wrap(bytePageNum).getInt() != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
			System.out.println("Completed scanning heapfile");
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *  Creates empty buckets and with RID and PageNumber set to -1.
	 *  EMPTY_HASH_INDICATOR is set to 0 to indicate that the records are empty.
	 *  The order in which each HashIndex is stored is: [EMPTY_HASH_INDICATOR, RID, PAGENUMBER, HASHKEY]
	 *  	all stored inside a byte array.
	 */
	
	public void createBuckets(FileOutputStream fos) {
		byte[] brid;
		byte[] bpagenum;
		byte[] bkey;
		byte[] hash_row = new byte[HASH_INDEX_SIZE];
		for (int i = 0; i < BUCKETS; i++) {
			for (int j = 0; j < BUCKET_SIZE; j++) {
				brid = ByteBuffer.allocate(INT_SIZE).putInt(-1).array(); //RID initialised as -1
				bpagenum = ByteBuffer.allocate(INT_SIZE).putInt(-1).array(); //PAGENUMBER initialised as -1
				bkey = ByteBuffer.allocate(INT_SIZE).putInt(i).array(); //Hash Key corresponding to bucket no.
				
				try {
					fos.write(hash_row);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 *  Inserts the offset pair for each record alongside the hash key.
	 *  The order in which each HashIndex is stored is: [EMPTY_HASH_INDICATOR, RID, PAGENUMBER, HASHKEY]
	 *  	all stored inside a byte array.
	*/
	
	public void insertKeyOffsetPair(byte[] hash, RandomAccessFile raf) {

		byte[] brid = Arrays.copyOfRange(hash, 0, 4);		
		byte[] bpage = Arrays.copyOfRange(hash, 4, 8);		
		byte[] bkey = Arrays.copyOfRange(hash, 8, 12);	
		int ikey = byteToInt(bkey); //Represents the bucket number stored in chronological order in hashfile.
		int bucketentry = 0;
		
		long insert_position =  moveToBucket(ikey); 
		try {
			raf.seek(insert_position);
			boolean searchBucket = true;	
			while (searchBucket) {
				bucketentry++;
				if (bucketentry >= BUCKET_SIZE) {
					writeOverflowBuckets(hash);
					searchBucket = false;
				}
			
				} else {
					raf.seek(raf.getFilePointer() + RID_SIZE + PAGE_SIZE + KEY_SIZE);
				}
			} 
		} catch (EOFException eof) {
		    System.err.println("Reached end of file");
		} catch (IOException e) {
		    System.err.println("Data-type can't be written to hashfile");
			e.printStackTrace();
		}
	}
	
	/**
	 *  Overflow buckets are appended to the bottom of the hash file.
	 */
	
	public void writeOverflowBuckets (byte[] hash) {
		try {
			FileOutputStream fos = new FileOutputStream(hashfile, true);
			fos.write(1); //Indicates full bucket
			hash_entries++;
			fos.write(hash, 0, 12);
			fos.close();
		} catch (FileNotFoundException e) {
		    System.err.println("Cannot locate hashfile");
		} catch (IOException e) {
		    System.err.println("Invalid file type");
			e.printStackTrace();
		}
	}
	
	public int estimatedOverflow() {
		return hash_entries;
	}

	public int moveToBucket(int hashkey) {
		return hashkey * BUCKET_SIZE * (RID_SIZE + PAGE_SIZE + KEY_SIZE + EMPTY_HASH_INDICATOR);
	}
	
	public int calculateBucketSize() {
		int no_of_rows_in_heap = (int) Math.ceil(heapfile.length() / RECORD_SIZE);
		int no_of_rows_in_hash = (int) Math.ceil(no_of_rows_in_heap / EXPECTED_OCCUPANCY);
		return no_of_rows_in_hash/BUCKETS;
	}
	
	public int getKey(byte[] bname) {
		return Math.abs(Arrays.hashCode(bname) % BUCKETS);
	}
	
	public int byteToInt(byte[] b) {
		return ByteBuffer.wrap(b).getInt();
	}
	
	public static void getStatistics () {
		System.out.println();
		System.out.println("Number of buckets created: "+ BUCKETS);
		System.out.println("BUCKET_SIZE: "+ BUCKET_SIZE);
		System.out.println("Number of records entered: "+ hash_entries);
	}
	
}

