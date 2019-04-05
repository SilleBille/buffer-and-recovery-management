package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;
import java.util.Iterator;

/**
 * A class that provides the ability to move through the records of the log file
 * in reverse order.
 * 
 * @author Edward Sciore
 */
public class LogIterator implements Iterator<BasicLogRecord> {
	private Block blk;
	private Page pg = new Page();
	private int currentrec;

	/**
	 * Creates an iterator for the records in the log file, positioned after the
	 * last log record. This constructor is called exclusively by
	 * {@link LogMgr#iterator()}.
	 */
	LogIterator(Block blk, int pos) {
		this.blk = blk;
		pg.read(blk);
		currentrec = pg.getInt(pos);
	}

	public Block currentBlock() {
		return blk;
	}

	/**
	 * Determines if the current log record is the earliest record in the log
	 * file.
	 * 
	 * @return true if there is an earlier record
	 */
	public boolean hasNext() {
		return currentrec > 0 || blk.number() > 0;
	}

	/**
	 * Moves to the next log record in reverse order. If the current log record
	 * is the earliest in its block, then the method moves to the next oldest
	 * block, and returns the log record from there.
	 * 
	 * @return the next earliest log record
	 */
	public BasicLogRecord next() {
		if (currentrec == 0)
			moveToNextBlock();
		currentrec = pg.getInt(currentrec);
		return new BasicLogRecord(pg, currentrec + INT_SIZE + INT_SIZE);
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Moves to the next log block in reverse order, and positions it after the
	 * last record in that block.
	 */
	private void moveToNextBlock() {
		blk = new Block(blk.fileName(), blk.number() - 1);
		pg.read(blk);
		currentrec = pg.getInt(LogMgr.LAST_POS);
	}

	// ======================= The Forward Iterator part starts here
	// ===================== //
	/**
	 * Moves to the next log record in forward order. If the current log record
	 * is the latest in its block, then the method moves to the next block, and
	 * returns the log record from there.
	 * 
	 * @return the next log record
	 */
	public BasicLogRecord nextForward() {

		// This denotes we have reached the end of the circular list. We will
		// know when we have hit the last block in the page
		if (currentrec == pg.getInt(LogMgr.LAST_POS) + INT_SIZE) {
			moveToNextForwardBlock();
		}

		// Here the data is present just after 1 block. So, add only INT_SIZE
		// once
		BasicLogRecord blockToReturn = new BasicLogRecord(pg, currentrec + INT_SIZE);
		currentrec = pg.getInt(currentrec);

		return blockToReturn;
	}

	/**
	 * Moves to the next log block in forward order, and positions it at the
	 * first record in that block.
	 */
	private void moveToNextForwardBlock() {

		blk = new Block(blk.fileName(), blk.number() + 1);
		pg.read(blk);
		int temp = pg.getInt(LogMgr.LAST_POS);
		currentrec = pg.getInt(temp + INT_SIZE);

	}

	/**
	 * Determines if the current log record is the latest record in the log
	 * file.
	 * 
	 * @return true if there is an latest record
	 */
	public boolean hasForwardNext() {
		return currentrec < pg.getInt(LogMgr.LAST_POS) + INT_SIZE || blk.number() < LogMgr.numberOfBlocks;
	}
}
