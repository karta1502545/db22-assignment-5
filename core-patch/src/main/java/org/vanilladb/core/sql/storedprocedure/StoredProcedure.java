/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.sql.storedprocedure;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.core.query.planner.BadSemanticException;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.PrimaryKey;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.LockAbortException;
import org.vanilladb.core.storage.tx.concurrency.conservative.ConservativeConcurrencyMgr;

/**
 * An abstract class that denotes the stored procedure supported in VanillaDb.
 */
public abstract class StoredProcedure<H extends StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(StoredProcedure.class
			.getName());
	
	private static final ReentrantLock SERIAL_CONTROL_LOCK = new ReentrantLock();
	
	private Transaction scheduleTransactionSerially(boolean isReadOnly,
			Set<PrimaryKey> readSet, Set<PrimaryKey> writeSet) {
		SERIAL_CONTROL_LOCK.lock();
		try {
			Transaction tx = VanillaDb.txMgr().newTransaction(
					Connection.TRANSACTION_SERIALIZABLE, isReadOnly);
			
			ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
			
			// Reserve lock so that deterministic ordering is ensured
			ccMgr.bookReadKeys(readSet);
			ccMgr.bookWriteKeys(writeSet);
			
			return tx;
		} finally {
			SERIAL_CONTROL_LOCK.unlock();
		}
	}
	
	protected Set<PrimaryKey> readSet = new HashSet<PrimaryKey>();
	protected Set<PrimaryKey> writeSet = new HashSet<PrimaryKey>();
	
	private H paramHelper;
	private Transaction tx;
	
	public StoredProcedure(H helper) {
		if (helper == null)
			throw new IllegalArgumentException("paramHelper should not be null");
		
		paramHelper = helper;
	}
	
	// Child classes of stored procedure should provide prepareKeys implementation
	protected abstract void prepareKeys();
	
	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);
		
		// Collect read/write sets
		prepareKeys();
		
		// create a transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		tx = scheduleTransactionSerially(isReadOnly, readSet, writeSet);
		
	}
	
	public SpResultSet execute() {
		boolean isCommitted = false;
		
		try {
			ConservativeConcurrencyMgr ccMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
			
			// Acquire locks before execution
			ccMgr.acquireBookedLocks();
			
			executeSql();
			
			// The transaction finishes normally
			tx.commit();
			isCommitted = true;
			
		} catch (LockAbortException lockAbortEx) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(lockAbortEx.getMessage());
			tx.rollback();
		} catch (ManuallyAbortException me) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Manually aborted by the procedure: " + me.getMessage());
			tx.rollback();
		} catch (BadSemanticException be) {
			if (logger.isLoggable(Level.SEVERE))
				logger.warning("Semantic error: " + be.getMessage());
			tx.rollback();
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
		}

		return new SpResultSet(
			isCommitted,
			paramHelper.getResultSetSchema(),
			paramHelper.newResultSetRecord()
		);
	}
	
	protected abstract void executeSql();
	
	protected H getParamHelper() {
		return paramHelper;
	}
	
	protected Transaction getTransaction() {
		return tx;
	}
	
	protected void abort() {
		throw new ManuallyAbortException();
	}
	
	protected void abort(String message) {
		throw new ManuallyAbortException(message);
	}
}