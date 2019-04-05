package simpledb.buffer;

import java.util.HashMap;
import simpledb.file.*;
import java.sql.Timestamp; //timestamp for fifo
import java.util.Date; //timestamp for fifo

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * 
 * @author Edward Sciore
 *
 */
public class BasicBufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private int totalCapacity;
	public HashMap<Block, Buffer> bufferPoolMap;

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} objects that it gets from the class
	 * {@link simpledb.server.SimpleDB}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 * 
	 * @param numbuffs
	 *            the number of buffer slots to allocate
	 */
	BasicBufferMgr(int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		totalCapacity = numbuffs;
		for (int i = 0; i < numbuffs; i++)
			bufferpool[i] = new Buffer();
		// CSC540
		bufferPoolMap = new HashMap<Block, Buffer>();

	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txnum
	 *            the transaction's id number
	 */
	synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a reference to a disk block
	 * @return the pinned buffer
	 */
	synchronized Buffer pin(Block blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null) {
				return null;
			}
			// CSC540 available buffer hashmap is of type buffer
			// if the unpinned buffer has an assignment, record the
			// de-assignment it in bufferTimeStampRecord
			if (buff.block() != null && buff.block() != blk) {
				bufferPoolMap.remove(buff.block());
			}
			buff.assignToBlock(blk);
			// Records a new assignment to buffer in bufferTimeStampRecord
			bufferPoolMap.put(blk, buff);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin();

		java.util.Date date = new java.util.Date(); // timestamp for fifo
		buff.timestamp = new Timestamp(date.getTime()); // timestamp for fifo

		return buff;

	}

	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
		Buffer buff = chooseUnpinnedBuffer();
		if (buff == null) {
				return null;
		} else {
			bufferPoolMap.remove(buff.block());
		}

		buff.assignToNew(filename, fmtr);

		numAvailable--;
		buff.pin();
		// Records a new assignment to buffer in bufferTimeStampRecord
		bufferPoolMap.put(buff.block(), buff);

		java.util.Date date = new java.util.Date(); // timestamp for fifo
		buff.timestamp = new Timestamp(date.getTime()); // timestamp for fifo

		return buff;
	}

	/**
	 * Unpins the specified buffer.
	 * 
	 * @param buff
	 *            the buffer to be unpinned
	 */
	synchronized void unpin(Buffer buff) {
		buff.unpin();
		if (!buff.isPinned())
			numAvailable++;
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable;
	}

	private Buffer findExistingBuffer(Block blk) {
		/*
		 * for (Buffer buff : bufferpool) { Block b = buff.block(); if (b !=
		 * null && b.equals(blk)) return buff; } return null;
		 */
		// CSC540
		Buffer existingBuffer = bufferPoolMap.get(blk);
		/*
		 * if(existingBuffer != null){
		 * System.out.println("A buffer is available");
		 * }else{System.out.println("None is available"); }
		 */
		return existingBuffer;
	}

	/*
	 * private Buffer chooseUnpinnedBuffer() { for (Buffer buff : bufferpool) if
	 * (!buff.isPinned()) return buff; return null; }
	 */
	public Buffer chooseUnpinnedBuffer() {
		Buffer FIFOBuff = null;
		for (Buffer buff : bufferpool) {
			if (!buff.isPinned()) {
				if (FIFOBuff == null) {
					FIFOBuff = buff;
				} else {
					if (buff.timestamp.before(FIFOBuff.timestamp)) {
						FIFOBuff = buff;
					}
				}
			}

		}
		/*
		 * if(FIFOBuff != null) System.out.println("Replacing: " +
		 * FIFOBuff.toString());
		 */
		return FIFOBuff;

	}

	boolean containsMapping(Block blk) {
		return bufferPoolMap.containsKey(blk);
	}

	Buffer getMapping(Block blk) {
		return bufferPoolMap.get(blk);
	}

}
