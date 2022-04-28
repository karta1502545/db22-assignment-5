package org.vanilladb.core.storage.tx.concurrency;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class ConservativeConcurrencyMgr extends ConcurrencyMgr {

	@Override
	public void onTxCommit(Transaction tx) {
		// TODO releaseAll locks
		
	}

	@Override
	public void onTxRollback(Transaction tx) {
		// TODO Auto-generated method stub
		
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

}
