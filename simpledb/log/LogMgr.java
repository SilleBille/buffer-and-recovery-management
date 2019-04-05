package simpledb.log;

import simpledb.server.SimpleDB;
import simpledb.file.*;
import static simpledb.file.Page.*;
import java.util.*;

/**
 * The low-level log manager. This log manager is responsible for writing log
 * records into a log file. A log record can be any sequence of integer and
 * string values. The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link simpledb.tx.recovery.RecoveryMgr recovery manager}.
 * 
 * @author Edward Sciore
 */
public class LogMgr implements Iterable<BasicLogRecord> {
	/**
	 * The location where the pointer to the last integer in the page is. A
	 * value of 0 means that the pointer is the first value in the page.
	 */
	public static final int LAST_POS = 0;
	public static final int FIRST_POS = 4;
	
	public static int numberOfBlocks;

	private String logfile;
	private Page mypage = new Page();
	private Block currentblk;
	private int currentpos;
	
	private int startBlock;

	// This is to have a copy of the total size of the objects appended
	private int previousForwardPointer = 0;

	/**
	 * Creates the manager for the specified log file. If the log file does not
	 * yet exist, it is created with an empty first block. This constructor
	 * depends on a {@link FileMgr} object that it gets from the method
	 * {@link simpledb.server.SimpleDB#fileMgr()}. That object is created during
	 * system initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileMgr(String)} is called first.
	 * 
	 * @param logfile
	 *            the name of the log file
	 */
	public LogMgr(String logfile) {
		this.logfile = logfile;
		int logsize = SimpleDB.fileMgr().size(logfile);
		if (logsize == 0)
			appendNewBlock();
		else {
			currentblk = new Block(logfile, logsize - 1);
			mypage.read(currentblk);

			// Here the int_size is incremented twice because now we are having
			// 2 pointers (one forward and one backward)
			currentpos = getLastRecordPosition() + INT_SIZE + INT_SIZE;
			// System.out.println("current position: " + currentpos);
		}
	}

	/**
	 * Ensures that the log records corresponding to the specified LSN has been
	 * written to disk. All earlier log records will also be written to disk.
	 * 
	 * @param lsn
	 *            the LSN of a log record
	 */
	public void flush(int lsn) {
		if (lsn >= currentLSN())
			flush();
	}

	/**
	 * Returns an iterator for the log records, which will be returned in
	 * reverse order starting with the most recent.
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	public synchronized Iterator<BasicLogRecord> iterator() {
		flush();
		return new LogIterator(currentblk, LAST_POS);
	}
	
	/**
	 * Returns an iterator for the log records, which will be returned in
	 * forward order starting with the most oldest.
	 * 
	 * @see java.lang.Iterable#iterator()
	 */
	public synchronized LogIterator forwardIterator(LogIterator revIter) {
		flush();
		Block revBlk = revIter.currentBlock();
		Block startBlock = new Block(revBlk.fileName(), revBlk.number());
		numberOfBlocks = currentblk.number();
		
		Page temp = new Page();
		temp.read(startBlock);
		int backCurrentRec =  temp.getInt(LogMgr.LAST_POS);
		int pos = backCurrentRec + INT_SIZE;
		//System.out.println("The first block number: " + temp.getInt(pos) + "  " +  temp.getInt(temp.getInt(pos)));
		return new LogIterator(startBlock, pos);
	}
	

	/**
	 * Appends a log record to the file. The record contains an arbitrary array
	 * of strings and integers. The method also writes an integer to the end of
	 * each log record whose value is the offset of the corresponding integer
	 * for the previous log record. These integers allow log records to be read
	 * in reverse order.
	 * 
	 * @param rec
	 *            the list of values
	 * @return the LSN of the final value
	 */
	public synchronized int append(Object[] rec) {
		int recsize = INT_SIZE + INT_SIZE; // 4 bytes for the integer that points to the
								// previous log record and 4 bytes for integer that points to next log record
		for (Object obj : rec)
			recsize += size(obj);
		if (currentpos + recsize >= BLOCK_SIZE) { // the log record doesn't fit,
			// System.out.println("Appending to a new Block");
			flush(); // so move to the next block.
			appendNewBlock();
		}
		for (Object obj : rec)
			appendVal(obj);

		previousForwardPointer = recsize;
		finalizeRecord();
		return currentLSN();
	}

	/**
	 * Adds the specified value to the page at the position denoted by
	 * currentpos. Then increments currentpos by the size of the value.
	 * 
	 * @param val
	 *            the integer or string to be added to the page
	 */
	private void appendVal(Object val) {
		if (val instanceof String) {
			mypage.setString(currentpos, (String) val);
		} else {
			// System.out.println("appendval for pos " + currentpos);
			mypage.setInt(currentpos, (Integer) val);
		}
		currentpos += size(val);
	}

	/**
	 * Calculates the size of the specified integer or string.
	 * 
	 * @param val
	 *            the value
	 * @return the size of the value, in bytes
	 */
	private int size(Object val) {
		if (val instanceof String) {
			String sval = (String) val;
			return STR_SIZE(sval.length());
		} else
			return INT_SIZE;
	}

	/**
	 * Returns the LSN of the most recent log record. As implemented, the LSN is
	 * the block number where the record is stored. Thus every log record in a
	 * block has the same LSN.
	 * 
	 * @return the LSN of the most recent log record
	 */
	private int currentLSN() {
		return currentblk.number();
	}

	/**
	 * Writes the current page to the log file.
	 */
	private void flush() {
		mypage.write(currentblk);
	}

	/**
	 * Clear the current page, and append it to the log file.
	 */
	private void appendNewBlock() {
		mypage.setInt(FIRST_POS, INT_SIZE);
		setLastRecordPosition(0);

		// Again, the starting also has 2 integers to hold one forward and one
		// backward
		currentpos = INT_SIZE + INT_SIZE;
		currentblk = mypage.append(logfile);
	}

	/**
	 * Sets up a circular chain of pointers to the records in the page. There is
	 * an integer added to the end of each log record whose value is the offset
	 * of the previous log record. The first four bytes of the page contain an
	 * integer whose value is the offset of the integer for the last log record
	 * in the page.
	 */
	private void finalizeRecord() {
		// Appends the integer after the record

		// 1. Update the previous forward pointer
		mypage.setInt(currentpos - previousForwardPointer + INT_SIZE, currentpos + INT_SIZE);
		// System.out.println("finalizing for pos:  " + currentpos);
		
		// 2. Set the last block of backward pointer to point to one previous
		mypage.setInt(currentpos, getLastRecordPosition());

		
		// 3. Set the last block of forward pointer to point forward
		mypage.setInt(currentpos + INT_SIZE, FIRST_POS);
		
		// 4. Set the first block value to the last record
		setLastRecordPosition(currentpos);
		currentpos += INT_SIZE + INT_SIZE;
	}

	private int getLastRecordPosition() {
		return mypage.getInt(LAST_POS);
	}

	private void setLastRecordPosition(int pos) {
		mypage.setInt(LAST_POS, pos);
	}
}
