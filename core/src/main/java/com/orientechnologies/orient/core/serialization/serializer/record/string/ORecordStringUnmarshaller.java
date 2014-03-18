package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

//TODO this parser is recursion based; it should be refactored and made stack based
public class ORecordStringUnmarshaller {

	int	i	= 0;

	public ORecordInternal<?> parse(String iContent, ORecordInternal<?> iRecord, String[] iFields) {
    iContent = iContent.trim();

		if (iContent.length() == 0)
      return iRecord;

    // UNMARSHALL THE CLASS NAME
    final ODocument record = (ODocument) iRecord;

    int pos;
    final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    final int posFirstValue = iContent.indexOf(OStringSerializerHelper.ENTRY_SEPARATOR);
    pos = iContent.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
    if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) {
      if ((record.getIdentity().getClusterId() < 0 || database == null || !database.getStorageVersions()
          .classesAreDetectedByClusterId()))
        record.setClassNameIfExists(iContent.substring(0, pos));
      iContent = iContent.substring(pos + 1);
    } else
      record.setClassNameIfExists(null);

    if (iFields != null && iFields.length == 1 && iFields[0].equals("@class"))
      // ONLY THE CLASS NAME HAS BEEN REQUESTED: RETURN NOW WITHOUT UNMARSHALL THE ENTIRE RECORD
      return iRecord;

		char[] chars = iContent.toCharArray();

		parseDocument(chars, record);

		return iRecord;
	}

	private OType getFieldType(ODocument iRecord, String fieldName) {
		if (iRecord.getSchemaClass() == null) {
			return null;
		}
		OProperty property = iRecord.getSchemaClass().getProperty(fieldName);
		if (property == null) {
			return null;
		}
		return property.getType();
	}

	private OType getFieldLinkedType(ODocument iRecord, String fieldName) {
		if (iRecord.getSchemaClass() == null) {
			return null;
		}
		OProperty property = iRecord.getSchemaClass().getProperty(fieldName);
		if (property == null) {
			return null;
		}
		return property.getLinkedType();
	}

	private Object parseFieldValue(char[] chars, OType type, OType linkedType, ORecord<?> iSourceRecord) {
		final char c = chars[i];
		if (c == OStringSerializerHelper.LINK) {
			return parseLink(chars);
		} else if (c == OStringSerializerHelper.LIST_BEGIN) {
			return parseList(chars, iSourceRecord, type, linkedType);
		} else if (c == OStringSerializerHelper.BAG_BEGIN) {
			return parseBag(chars, iSourceRecord);
		} else if (c == OStringSerializerHelper.EMBEDDED_BEGIN) {
			return parseEmbedded(chars);
		} else if (c == OStringSerializerHelper.MAP_BEGIN) {
			return parseMap(chars, iSourceRecord, type, linkedType);
		} else if (c == OStringSerializerHelper.SET_BEGIN) {
			return parseSet(chars, iSourceRecord, type, linkedType);
		} else if (c == OStringSerializerHelper.RECORD_SEPARATOR) {
			return null;
		} else if (c == OStringSerializerHelper.EMBEDDED_END) {
			return null;
		} else if (c == 'f' && chars[i + 1] == 'a' && chars[i + 2] == 'l') {
			i += 5;
			return false;
		} else if (c == 't' && chars[i + 1] == 'r' && chars[i + 2] == 'u') {
			i += 4;
			return true;
		} else if (isNumber(c)) {
			return parseNumberAndDateValue(chars, type);
		} else {
			return parseStringFieldValue(chars);
		}

		// TODO binary!!!
	}

	private boolean isNumber(char c) {
		return c == '-' || c == '.' || (c <= '9' && c >= '0');
	}

	private Object parseNumberAndDateValue(char[] chars, OType type) {
		StringBuilder builder = new StringBuilder();
		for (; i < chars.length; i++) {
			char c = chars[i];
			if (!isNumber(c) && !(c == 'b' || c == 's' || c == 'l' || c == 'f' || c == 'd' || c == 'c' || c == 'a' || c == 't')) {
				return ORecordSerializerStringAbstract.getTypeValue(builder.toString());
			}
			builder.append(c);
		}
		return ORecordSerializerStringAbstract.getTypeValue(builder.toString());
	}

	private Object parseList(char[] chars, ORecord<?> iSourceRecord, OType type, OType linkedtType) {
		if (type == OType.LINKLIST) {
			StringBuilder builder = new StringBuilder();
			i++;
			if (i < chars.length && chars[i] == OStringSerializerHelper.LIST_END) {
				i++;
				return new ORecordLazyList((ODocument) iSourceRecord).setStreamedContent(builder);
			}
			while (i < chars.length && chars[i] != OStringSerializerHelper.LIST_END) {
				builder.append(chars[i]);
				if (i < chars.length && chars[i] == OStringSerializerHelper.LIST_END) {
					i++;
					break;
				}
				i++;
			}
			return new ORecordLazyList((ODocument) iSourceRecord).setStreamedContent(builder);
		} else {
			List<Object> result = new OTrackedList<Object>(iSourceRecord);
			i++;
			if (i < chars.length && chars[i] == OStringSerializerHelper.LIST_END) {
				i++;
				return result;
			}
			while (i < chars.length && chars[i] != OStringSerializerHelper.LIST_END) {
				result.add(parseFieldValue(chars, linkedtType, null, iSourceRecord));
				if (i < chars.length && chars[i] == OStringSerializerHelper.LIST_END) {
					i++;
					return result;
				}
				i++;
			}
			return result;
		}
	}

	private Object parseSet(char[] chars, ORecord<?> iSourceRecord, OType type, OType linkedtType) {
		if (type == OType.LINKSET) {
			StringBuilder builder = new StringBuilder();
			i++;
			if (i < chars.length && chars[i] == OStringSerializerHelper.SET_END) {
				i++;
				return new OMVRBTreeRIDSet(iSourceRecord).fromStream(builder);
			}
			while (i < chars.length && chars[i] != OStringSerializerHelper.SET_END) {
				builder.append(chars[i]);
				if (i < chars.length && chars[i] == OStringSerializerHelper.SET_END) {
					i++;
					break;
				}
				i++;
			}
			return new OMVRBTreeRIDSet(iSourceRecord).fromStream(builder);
		} else {
			Set<Object> result = new OTrackedSet<Object>(iSourceRecord);
			i++;
			if (i < chars.length && chars[i] == OStringSerializerHelper.SET_END) {
				i++;
				return result;
			}
			while (i < chars.length && chars[i] != OStringSerializerHelper.SET_END) {
				result.add(parseFieldValue(chars, linkedtType, null, iSourceRecord));
				if (i < chars.length && chars[i] == OStringSerializerHelper.SET_END) {
					i++;
					return result;
				}
				i++;
			}

			return result;
		}
	}

	private Object parseMap(char[] chars, ORecord<?> iSourceRecord, OType type, OType iLinkedType) {
		Map result = new OTrackedMap(iSourceRecord);
    if (iLinkedType == OType.LINK || iLinkedType == OType.EMBEDDED){
    	result = new ORecordLazyMap((ODocument)iSourceRecord, ODocument.RECORD_TYPE);
    }else{
    	result = new OTrackedMap(iSourceRecord);
    }

		i++;
		if (i < chars.length && chars[i] == OStringSerializerHelper.MAP_END) {
			i++;
			return result;
		}
		while (i < chars.length && chars[i] != OStringSerializerHelper.MAP_END) {
			Object key = parseFieldValue(chars, null, null, iSourceRecord);
			i++;
			Object value = parseFieldValue(chars, null, null, iSourceRecord);
			result.put(key, value);
			if (i < chars.length && chars[i] == OStringSerializerHelper.MAP_END) {
				i++;
				return result;
			}
			i++;
		}
		return result;
	}

	private Object parseEmbedded(char[] chars) {
		ODocument record = new ODocument();
		i++;
		parseDocument(chars, record);
		return record;
	}

	private void parseDocument(char[] chars, ODocument record) {

		if (i < chars.length && chars[i] == OStringSerializerHelper.EMBEDDED_END) {
			i++;
			return;
		}
		while (i < chars.length && chars[i] != OStringSerializerHelper.EMBEDDED_END) {
			String fieldName = parseFieldName(chars);
			if (chars[i] == OStringSerializerHelper.CLASS_SEPARATOR.charAt(0)) {
				record.setClassNameIfExists(fieldName);
			} else {
				i++;
				Object fieldValue = parseFieldValue(chars, getFieldType(record, fieldName), getFieldLinkedType(record, fieldName), record);
				try {
					record.field(fieldName, fieldValue);
				} catch (Exception e) {
					// System.out.println(i);
				}
				if (i < chars.length && chars[i] == OStringSerializerHelper.EMBEDDED_END) {
					i++;
					return;
				}
			}
			i++;
		}
	}

	private Object parseBag(char[] chars, ORecord<?> iSourceRecord) {
		i++;
		if (i < chars.length && chars[i] == OStringSerializerHelper.SET_END) {
			i++;
			return null;
		}
		StringBuilder builder = new StringBuilder();
		while (i < chars.length && chars[i] != OStringSerializerHelper.SET_END) {
			builder.append(chars[i]);
			i++;
		}
		return ORidBag.fromStream(builder.toString());
	}

	private Object parseStringFieldValue(char[] chars) {
		char beginChar = chars[i++];
		if (beginChar == OStringSerializerHelper.EMBEDDED_END || beginChar == OStringSerializerHelper.LIST_END
				|| beginChar == OStringSerializerHelper.SET_END || beginChar == OStringSerializerHelper.MAP_END
				|| beginChar == OStringSerializerHelper.BAG_END) {
			i++;
			return null;
		}
		StringBuilder builder = new StringBuilder();
		boolean quote = false;
		while (i < chars.length && (chars[i] != beginChar || quote)) {
			if (chars[i] == '\\' && !quote) {
				quote = true;
				i++;
				continue;
			}
			builder.append(chars[i++]);
			quote = false;
		}
		i++;
		return builder.toString();
	}

	private Object parseLink(char[] chars) {
		i++;
		StringBuilder builder = new StringBuilder();
		boolean foundSeparator = false;
		while (i < chars.length && ((chars[i] <= '9' && chars[i] >= '0') || (chars[i] == ':' && !foundSeparator))) {
			if (chars[i] == ':') {
				foundSeparator = true;
			}
			builder.append(chars[i]);
			i++;
		}
		return new ORecordId(builder.toString());
	}

	private String parseFieldName(char[] chars) {
		StringBuilder builder = new StringBuilder();
		for (; i < chars.length; i++) {
			if (chars[i] == ORecordSerializerSchemaAware2CSV.FIELD_VALUE_SEPARATOR
					|| chars[i] == OStringSerializerHelper.CLASS_SEPARATOR.charAt(0)) {
				return builder.toString();
			}
			builder.append(chars[i]);
		}
		return builder.toString();
	}

}
