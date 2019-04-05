Team Members:
=============

* Dinesh Prasanth Moluguwan Krishnamoorthy – dmolugu
* Tyler Cannon – tjcannon
* Tianpei Xia – txia4
* Okoilu Ruth – rookoilu


List of Files Changed:
======================
* BasicBufferMgr.java
* Buffer.java
* LogIterator.java
* LogMgr.java
* CheckpointRecord.java
* CommitRecord.java
* LogRecord.java
* LogRecordForwardIterator.java  //the new class we added
* LogRecordIterator.java
* RecoveryMgr.java
* RollbackRecord.java
* SetIntRecord.java
* SetStringRecord.java
* StartRecord.java


Sample code for Testing Iterator:
=================================
	SimpleDB.init("simpleDB");
	Block blk1 = new Block("filename", 1);
	Block blk2 = new Block("filename 2", 4);

	BufferMgr bm = SimpleDB.bufferMgr();

	Buffer buff1 = bm.pin(blk1);
	Buffer buff2 = bm.pin(blk2);

	RecoveryMgr rm = new RecoveryMgr(123);

	int lsn = rm.setInt(buff1, 4, 1234);
	buff1.setInt(4, 1234, 123, lsn);

	int lsn2 = rm.setString(buff2, 10, "dinesh");
	buff2.setString(10, "dinesh", 123, lsn2);

	rm.commit();



	LogRecordIterator iter = new LogRecordIterator();
	while(iter.hasNext()) {
		System.out.println("Backward reading: " + iter.next());
		
	}

	LogRecordForwardIterator forwardIter = new LogRecordForwardIterator(iter);
	while(forwardIter.hasNext()) {
		System.out.println("Forward Reading: " + forwardIter.next());
	}

Sample Code to test Recovery: 
=============================

	SimpleDB.init("abcd");

	bm = SimpleDB.bufferMgr();

	Buffer buff = bm.pin(blk);

	buff.setInt(14, 4444, 1, -1);
	buff.setString(32, "oldvalue", 1, -1);
	
	bm.unpin(buff);
	
	
	
	
	// Can't use 1 as it's being used for intialization
	int t1 = 22;
	int t2 = 33;
	RecoveryMgr rm1 = new RecoveryMgr(t1);
	RecoveryMgr rm2 = new RecoveryMgr(t2);

	buff = bm.pin(blk);
	
	int lsn = rm1.setInt(buff, 14, 1234);
	buff.setInt(14, 1234, t1, lsn);
	
	lsn = rm2.setString(buff, 32, "newvalue");
	buff.setString(32, "newvalue", t2, lsn);
	
	bm.unpin(buff);

	if (buff.getInt(14) == 1234 && buff.getString(32).equals("newvalue"))
		System.out.println("New values are set");

	bm.unpin(buff);
	rm2.commit();
	
	rm1.recover(); 
	buff = bm.pin(blk);
	if (buff.getInt(14) == 4444 && buff.getString(32).equals("newvalue"))
		System.out.println("Recovered properly");

	bm.unpin(buff);
