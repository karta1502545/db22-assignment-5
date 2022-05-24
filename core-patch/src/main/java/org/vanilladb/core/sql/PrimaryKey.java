package org.vanilladb.core.sql;

import java.util.Map;

public class PrimaryKey {
	
	private String tableName;
	private Map<String, Constant> keyEntryMap;
	private int hashCode;
	
	public PrimaryKey(String tableName, Map<String, Constant> keyEntryMap) {
		this.tableName = tableName;
		this.keyEntryMap = keyEntryMap;
		
		genHashCode();
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public Constant getKeyVal(String fld) {
		return keyEntryMap.get(fld);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != PrimaryKey.class)
			return false;
		PrimaryKey key = (PrimaryKey) obj;
		return key.tableName.equals(this.tableName) && key.keyEntryMap.equals(this.keyEntryMap);
	}

	@Override
	public int hashCode() {
		return hashCode;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append(tableName);
		sb.append(": ");
		for (Map.Entry<String, Constant> entry : keyEntryMap.entrySet()) {
			sb.append(entry.getKey());
			sb.append(" -> ");
			sb.append(entry.getValue());
			sb.append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}");
		return sb.toString();
	}

	private void genHashCode() {
		hashCode = 17;
		hashCode = 31 * hashCode + tableName.hashCode();
		hashCode = 31 * hashCode + keyEntryMap.hashCode();
	}
}
