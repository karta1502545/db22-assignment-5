/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.bench.server.procedure.micro;

import java.util.ArrayList;
import java.util.HashMap;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.storage.tx.concurrency.conservative.ConservativeConcurrencyMgr;

public class MicroTxnProc extends StoredProcedure<MicroTxnProcParamHelper> {

	private static HashMap<Integer, RecordId> ItemIdToRecordId = new HashMap<Integer, RecordId>();
	private static Boolean isBulitMapping = false;
	private ArrayList<RecordId> readRecordIdList = new ArrayList<RecordId>();
	private ArrayList<RecordId> writeRecordIdList = new ArrayList<RecordId>();

	public MicroTxnProc() {
		super(new MicroTxnProcParamHelper());
	}

	public synchronized void bulidItemIdToRecordIdMapping() {
		if (isBulitMapping) return;
		isBulitMapping = true;
		Transaction tx = getTransaction();
		TableInfo tblInfo = VanillaDb.catalogMgr().getTableInfo("item", tx);
		RecordFile recordFile = tblInfo.open(tx, false);
		recordFile.beforeFirst();
		while(recordFile.next()) {
			int itemId = (int) recordFile.getVal("i_id").asJavaVal();
			ItemIdToRecordId.put(itemId, recordFile.currentRecordId());
		}
		// for(Integer k: ItemIdToRecordId.keySet())
		// 	System.out.println("key: " + k + ", value: " + ItemIdToRecordId.get(k));
	}

	public void getReadWriteItemRecordIds() { // TODO: tx # 4 cannot get its corresponding recordIds
		// while (!isBulitMapping) ; // busy waiting until the mapping is built
		try {
			MicroTxnProcParamHelper paramHelper = getParamHelper();
			int[] readItemIdList = paramHelper.getReadItemIdList();
			int[] writeItemIdList = paramHelper.getWriteItemIdList();
			for(int i=0; i<readItemIdList.length; i++) {
				if (ItemIdToRecordId.get(readItemIdList[i]) == null) {
					// for(int j=0; j<readItemIdList.length; j++)
					// 	System.out.println(readItemIdList[j]);
					// System.out.println("read i_id = " + readItemIdList[i] + " corresponding recordId is " + ItemIdToRecordId.get(readItemIdList[i]));
				}
				else {
					readRecordIdList.add(ItemIdToRecordId.get(readItemIdList[i]));
				}
			}
			for(int i=0; i<writeItemIdList.length; i++) {
				if (ItemIdToRecordId.get(writeItemIdList[i]) == null) {
					for(int j=0; j<writeItemIdList.length; j++)
						System.out.println(writeItemIdList[j]);
					System.out.println("txNum = " + getTransaction().getTransactionNumber());
					System.out.println("write i_id = " + writeItemIdList[i] + " corresponding recordId is " + ItemIdToRecordId.get(writeItemIdList[i]));
				}
				else {
					writeRecordIdList.add(ItemIdToRecordId.get(writeItemIdList[i]));
				}
			}
		} catch(Exception e) {
			// throw new RuntimeException(String());
		}
	}

	@Override
	protected void executeSql() {
		MicroTxnProcParamHelper paramHelper = getParamHelper();
		Transaction tx = getTransaction();
		// TODO
		// ERROR: org.vanilladb.core.storage.tx.concurrency.SerializableConcurrencyMgr 
		//        cannot be cast to org.vanilladb.core.storage.tx.concurrency.conservative.ConservativeConcurrencyMgr
		ConservativeConcurrencyMgr conserConcurMgr = (ConservativeConcurrencyMgr) tx.concurrencyMgr();
		bulidItemIdToRecordIdMapping();
		// get all Records this tx needs
		// while (!isBulitMapping) {
		// 	// busy waiting
		// 	;
		// }
		getReadWriteItemRecordIds();

		// get all locks
		try	{
			conserConcurMgr.lockReadWriteRecordIds(readRecordIdList, writeRecordIdList);
		} catch (Exception e) {
			e.printStackTrace();
			tx.rollback();
		}

		
		// SELECT
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			Scan s = StoredProcedureHelper.executeQuery(
				"SELECT i_name, i_price FROM item WHERE i_id = " + iid,
				tx
			);
			s.beforeFirst();
			if (s.next()) {
				String name = (String) s.getVal("i_name").asJavaVal();
				double price = (Double) s.getVal("i_price").asJavaVal();

				paramHelper.setItemName(name, idx);
				paramHelper.setItemPrice(price, idx);
			} else
				throw new RuntimeException("Cloud not find item record with i_id = " + iid);

			s.close();
		}
		
		// UPDATE
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			StoredProcedureHelper.executeUpdate(
				"UPDATE item SET i_price = " + newPrice + " WHERE i_id =" + iid,
				tx
			);
		}
	}
}

/*
tx 1:
Read:  0-9   S-Lock
Write: 0-6   X-Lock

tx2:
Read: 7-13   S-Lock
Write: 10-20 X-Lock
*/