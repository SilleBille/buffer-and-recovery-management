package simpledb.tx.recovery;

import static simpledb.tx.recovery.LogRecord.CHECKPOINT;
import static simpledb.tx.recovery.LogRecord.COMMIT;
import static simpledb.tx.recovery.LogRecord.ROLLBACK;
import static simpledb.tx.recovery.LogRecord.SETINT;
import static simpledb.tx.recovery.LogRecord.SETSTRING;
import static simpledb.tx.recovery.LogRecord.START;

import java.util.Iterator;

import simpledb.log.BasicLogRecord;
import simpledb.log.LogIterator;
import simpledb.server.SimpleDB;

/**
 * A class that provides the ability to read records from the log in forward
 * order. Unlike the similar class {@link simpledb.log.LogIterator LogIterator},
 * this class understands the meaning of the log records.
 * 
 * @author Dinesh Prasanth
 */
public class LogRecordForwardIterator implements Iterator<LogRecord> {

	private LogIterator forwardIter;

	public LogRecordForwardIterator(LogRecordIterator revIter) {
		// get the LogIterator from LogRecordIterator
		forwardIter = SimpleDB.logMgr().forwardIterator(revIter.iter());
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasNext() {
		return forwardIter.hasForwardNext();
	}

	@Override
	public LogRecord next() {
		BasicLogRecord r = forwardIter.nextForward();
		int op = r.nextInt();
		switch (op) {
		case CHECKPOINT:
			return new CheckpointRecord(r);
		case START:
			return new StartRecord(r);
		case COMMIT:
			return new CommitRecord(r);
		case ROLLBACK:
			return new RollbackRecord(r);
		case SETINT:
			return new SetIntRecord(r);
		case SETSTRING:
			return new SetStringRecord(r);
		default:
			return null;
		}
	}

}
