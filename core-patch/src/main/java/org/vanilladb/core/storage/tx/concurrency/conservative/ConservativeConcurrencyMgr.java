package org.vanilladb.core.storage.tx.concurrency.conservative;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import java.util.ArrayList;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;


public class ConservativeConcurrencyMgr extends ConcurrencyMgr {
	private int grantedTxNum = 3;

	public ConservativeConcurrencyMgr(long txNumber) {
		txNum = txNumber;
	}

	public synchronized void lockReadWriteRecordIds(ArrayList<RecordId> readRecordIds, 
									   ArrayList<RecordId> writeRecordIds) throws InterruptedException {
		// smaller tx number should get its locks earlier.
		// only one tx can access this block in a specific time.
		// while (txNum != grantedTxNum) {
		// 	System.out.println(txNum + "is waiting for lock...");
		// 	wait();
		// }
		// System.out.println(txNum + "is locking...");
		// get the locks of readRecordIds and writeRecordIds
			for (int i=0; i<readRecordIds.size(); i++) {
				RecordId recId = readRecordIds.get(i);
				try {
					lockTbl.sLock(recId, txNum);
				} catch(Exception e) {
					System.out.println("ReadRecordIds : " + readRecordIds.toString());
					System.out.println(txNum + ": " + recId + " getReadLock failed.");
				}
			}
			for (int i=0; i<writeRecordIds.size(); i++) {
				RecordId recId = writeRecordIds.get(i);
				try {
					lockTbl.xLock(recId, txNum);
				} catch(Exception e) {
					System.out.println(txNum + ": " + recId + " getWriteLock failed.");
				}
			}
			// System.out.println(txNum + "is finished locking...");
			grantedTxNum += 1;
			notifyAll();
	}

	@Override
	public void onTxCommit(Transaction tx) {
		// TODO releaseAll locks
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxRollback(Transaction tx) {
		// TODO Auto-generated method stub
		lockTbl.releaseAll(txNum, false);
	}

	@Override
	public void onTxEndStatement(Transaction tx) {
		// TODO Auto-generated method stub

	}

	@Override
	public void modifyFile(String fileName) {
		// TODO Auto-generated method stub
	}

	@Override
	public void readFile(String fileName) {
		// TODO Auto-generated method stub
	}

	@Override
	public void insertBlock(BlockId blk) {
		// TODO Auto-generated method stub
	}

	@Override
	public void modifyBlock(BlockId blk) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readBlock(BlockId blk) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void modifyRecord(RecordId recId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void readRecord(RecordId recId) {
		// TODO Auto-generated method stub
	}

	@Override
	public void modifyIndex(String dataFileName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readIndex(String dataFileName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void modifyLeafBlock(BlockId blk) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void readLeafBlock(BlockId blk) {
		// TODO Auto-generated method stub
		
	}
}
