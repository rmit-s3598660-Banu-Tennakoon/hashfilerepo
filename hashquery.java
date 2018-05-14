package heapfile;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * This class looks for a record in a heap file using the business name as a hash index.
 * @param business name of record searched.
 * @param page size of the heapfile.
 * @return hash file built on the specified heapfile.
 */

public class hashquery implements hashimpl{

	public static int BUCKET_SIZE;
	public File hashfile;
	public File heapfile;
	
	public static void main(String[] args) {

		hashquery load = new hashquery();
		
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();
		
		System.out.println("Time taken: " + (endTime-startTime) + "ms");
	}

	public void readArguments(String args[]) {
		try {
			readRecord(args[0], Integer.parseInt(args[1]));
		} catch (NumberFormatException e) {
		    System.err.println("Please a valid business name and the page number as arguments");
		}
	}
	
	/**
	 * This method uses the business name to match to a key-offset pair in the hash table.
	 * This is then used to locate the record in the heapfile.
	 */
	
	public void readRecord(String businessname, int pagesize) {
		heapfile = new File (HEAP_FNAME + pagesize);
		hashfile = new File (HASH_NAME + pagesize);

		BUCKET_SIZE = calculateBucketSize();
		
		try {				
			RandomAccessFile rafhash = new RandomAccessFile(hashfile, "r");			
			RandomAccessFile rafheap = new RandomAccessFile(heapfile, "r");			
		
			byte[] bname;
			byte[] key;
			byte[] bnametosearch = new byte[BN_NAME_SIZE];
			int bkeyToSearch;

			bname = businessname.trim().getBytes(ENCODING);
			System.arraycopy(bname, 0, bnametosearch, 0, bname.length); //business name needs to be in a
			key = ByteBuffer.allocate(INT_SIZE).putInt(getKey(bnametosearch)).array();
			bkeyToSearch = ByteBuffer.wrap(key).getInt();
			
			long seekpos =  moveToBucket(bkeyToSearch);
			System.out.println("BUCKET SIZE: " + BUCKET_SIZE);
			boolean search = true;
			rafhash.seek(rafhash.getFilePointer()+seekpos+1);
			
			int bucket_position = 0;
			
			while (search) {
				bucket_position++;
				int rid = rafhash.readInt();
				int pagenum = rafhash.readInt();			
				rafhash.readInt();
				int nextpos = pagenum * pagesize + RECORD_SIZE * rid;
				rafheap.seek(nextpos);
				byte[] rec = new byte[RECORD_SIZE];
				rafheap.read(rec, 0, RECORD_SIZE);
				byte[] recname = getNameFromRecord(rec);
				
				String srecname = new String(recname).trim();
				String sbname = new String(bname).trim();
				
				if (srecname.equals(sbname)) {
					String fullrecord = new String(rec);
					System.out.println(fullrecord);
					search = false;
					if (bucket_position == BUCKET_SIZE) {
						int seekposition_overflow = moveToBucket(BUCKETS); //Move past the last indexed bucket
						rafhash.seek(seekposition_overflow);
					}
				}
				rafhash.seek(rafhash.getFilePointer() + 1);
			}			
			rafhash.close();	
			rafheap.close();				
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException fnf) {
		    System.err.println("Heap file / Hash file is missing");
		} catch (EOFException eof) {
		    System.err.println("Reached end of hash file. Business name could not be found.");			
		} catch (IOException e) {
		    System.err.println("Please a valid business name and the page number as arguments.");
		}	
	}

	public byte[] getNameFromRecord(byte[] rec) {
		byte[] recname = new byte[BN_NAME_SIZE];
		System.arraycopy(rec, BN_NAME_OFFSET, recname, 0, BN_NAME_SIZE);
		return recname;
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
	
	
}
		