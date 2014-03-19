/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
 */
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ODocumentSerializer;

/**
 * Binary implementation of record serializer. It works lazy against byte[] and let un-marshall fields at the fly.
 * 
 * <pre>
 * +---------------------------------------------------------------------+---------------------+
 * |                          SCHEMA-FULL SECTION                        |                     |
 * +------+-----------------------------------+--------------------------| SCHEMA-LESS SECTION |
 * | SIZE | FIXED-CONTENT SECTION             | VARIABLE-CONTENT SECTION |                     |
 * |      |                                   |                          |                     |
 * |      | This is the schema-driven section | This contains var size   |                     |
 * |      |                                   | contents                 |                     |
 * +------+-----------------------------------+--------------------------+---------------------+
 * </pre>
 * 
 * Schema-full section is schema driven, while schema-less can have arbitrary fields.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORecordSerializerDocument2Binary implements ODocumentSerializer {
  public static final String NAME = "doc2bin";

  @Override
  public boolean supportsPartial() {
    return true;
  }

  public ORecordInternal<?> fromStream(final ODatabaseRecord iDatabase, final byte[] iSource) {
    return fromStream(iSource, null, null);
  }

  @Override
  public ODocument fromStream(final byte[] iSource, final ORecordInternal<?> iRecord, final String[] iFieldNames) {
    ODocument record = (ODocument) iRecord;
    if (record == null)
      record = new ODocument();

    record.fromStream(iSource);

    return record;
  }

  @Override
  public Object fieldFromStream(final byte[] iSource, final ODocument iRecord, final String iFieldName) {
    final OClass cls = iRecord.getSchemaClass();
    if (cls != null) {
      final OProperty prop = cls.getProperty(iFieldName);
      if (prop != null) {
        // SCHEMA-FULL: GET THE VALUE
        final OBinarySerializer<Object> serializer = OBinarySerializerFactory.getInstance().getObjectSerializer(prop.getType());
        final int pOffset = prop.getPersistentOffset();
        if (serializer.isFixedLength())
          return serializer.deserialize(iSource, pOffset);
        else {
          // READ THE VAR OFFSET
          final int varOffset = OIntegerSerializer.INSTANCE.deserialize(iSource, pOffset);
          return serializer.deserialize(iSource, varOffset);
        }
      }
    }

    // SCHEMA-LESS SECTION
    int fieldOffset = OIntegerSerializer.INSTANCE.deserialize(iSource, 0);

    while (fieldOffset < iSource.length) {
      // TODO: AVOID TO UN-MARSHALL THE ENTIRE FIELD NAME BUT RATHER START MATCHING FROM THE SIZE AND BYTES ONE BY ONE
      final String fieldName = OStringSerializer.INSTANCE.deserialize(iSource, fieldOffset);

      fieldOffset += OStringSerializer.INSTANCE.getObjectSize(fieldName);
      final OType currentType = OType.getById(OByteSerializer.INSTANCE.deserialize(iSource, fieldOffset));

      final OBinarySerializer<Object> currentSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(currentType);

      if (fieldName.equals(iFieldName))
        return currentSerializer.deserialize(iSource, fieldOffset);

      fieldOffset += currentSerializer.getObjectSize(iSource, fieldOffset + OByteSerializer.BYTE_SIZE);
    }

    return null;
  }

  @Override
  public byte[] fieldToStream(final byte[] iSource, final ODocument iRecord, final String iFieldName, final Object iFieldValue,
      final OType iFieldType) {
    int schemaLessSectionOffset = OIntegerSerializer.INT_SIZE;

    final OClass cls = iRecord.getSchemaClass();
    if (cls != null) {
      if (iSource == null) {
        // FIRST TIME: CREATE ENOUGH SPACE TO HOST FIXED SIZE
        schemaLessSectionOffset += cls.getPersistentSize();

        // TODO: USE OVERSIZE HERE?
        OIntegerSerializer.INSTANCE.serialize(schemaLessSectionOffset, iSource, 0);
      } else
        schemaLessSectionOffset = OIntegerSerializer.INSTANCE.deserialize(iSource, 0);

      final OProperty prop = cls.getProperty(iFieldName);
      if (prop != null) {
        // SCHEMA-FULL: GET THE VALUE
        final OBinarySerializer<Object> serializer = OBinarySerializerFactory.getInstance().getObjectSerializer(prop.getType());
        final int pOffset = prop.getPersistentOffset();
        if (serializer.isFixedLength())
          // FIXED SIZE: OVERWRITE IT
          serializer.serialize(iFieldValue, iSource, pOffset);
        else {
          // READ THE VAR OFFSET
          final int varOffset = OIntegerSerializer.INSTANCE.deserialize(iSource, pOffset);

          // CHECK IF THE SIZE IS THE SAME
          final int varSize = OIntegerSerializer.INSTANCE.deserialize(iSource, varOffset);
          final int newSize = serializer.getObjectSize(iFieldValue);
          if (newSize <= varSize)
            // SAME OR LESS SIZE: OVERWRITE IT. AVOID COMPACTION IF LESS
            serializer.serialize(iFieldValue, iSource, varOffset);
          else {
            // BIGGER SIZE: MAKE ROOM AND SHIFT TO RIGHT NEXT VAR-CONTENTS
            final byte[] newBuffer = updateVarContentInSchemaFullSection(iSource, varOffset, varSize, newSize, cls,
                schemaLessSectionOffset, serializer, iFieldValue);

            // UPDATE SCHEMA-LESS SECTION OFFSET
            schemaLessSectionOffset += varSize;
            OIntegerSerializer.INSTANCE.serialize(schemaLessSectionOffset, iSource, 0);

            return newBuffer;
          }
        }
        return iSource;
      }
    }

    final OBinarySerializer<Object> newSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(iFieldType);
    if (newSerializer == null)
      throw new IllegalArgumentException("no available binary serializer for type: " + iFieldType);

    final int newSize = newSerializer.getObjectSize(iFieldValue);

    // SCHEMA-LESS SECTION
    int fieldOffset = schemaLessSectionOffset;
    while (fieldOffset < iSource.length) {

      // TODO: AVOID TO UN-MARSHALL THE ENTIRE FIELD NAME BUT RATHER START MATCHING FROM THE SIZE AND BYTES ONE BY ONE
      final String fieldName = OStringSerializer.INSTANCE.deserialize(iSource, fieldOffset);

      fieldOffset += OStringSerializer.INSTANCE.getObjectSize(fieldName);
      final OType currentType = OType.getById(OByteSerializer.INSTANCE.deserialize(iSource, fieldOffset));

      final OBinarySerializer<Object> currentSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(currentType);
      final int currentFieldSize = currentSerializer.getObjectSize(iSource, fieldOffset + OByteSerializer.BYTE_SIZE);

      if (fieldName.equals(iFieldName)) {
        if (!currentType.equals(iFieldType))
          // TYPE CHANGED, OVERWRITE IT
          OByteSerializer.INSTANCE.serialize((byte) iFieldType.getId(), iSource, fieldOffset);

        fieldOffset++;

        if (newSize == currentFieldSize)
          // SAME SIZE: OVERWRITE IT
          newSerializer.serialize(iFieldValue, iSource, fieldOffset);
        else
          // DIFFERENT SIZE: COMPACT RECORD
          return updateVarContentInSchemaLessSection(iSource, fieldOffset, currentFieldSize, newSize, newSerializer, iFieldValue);

        return iSource;
      }

      fieldOffset += currentFieldSize;
    }

    // NOT FOUND, APPEND IT
    final int fieldNameSize = OStringSerializer.INSTANCE.getObjectSize(iFieldName);

    final byte[] newBuffer = new byte[iSource.length + fieldNameSize + OByteSerializer.BYTE_SIZE + newSize];

    // COPY DATA BEFORE OFFSET
    System.arraycopy(iSource, 0, newBuffer, 0, iSource.length);

    // SERIALIZE FIELD NAME
    OStringSerializer.INSTANCE.serialize(iFieldName, iSource, fieldOffset);
    fieldOffset += fieldNameSize;

    // SERIALIZE FIELD TYPE
    OByteSerializer.INSTANCE.serialize((byte) iFieldType.getId(), iSource, fieldOffset);
    fieldOffset++;

    // SERIALIZE FIELD VALUE
    newSerializer.serialize(iFieldValue, iSource, fieldOffset);

    return newBuffer;
  }

  @Override
  public byte[] toStream(final ORecordInternal<?> iRecord, final boolean iOnlyDelta) {
    byte[] buffer = null;

    ODocument document = (ODocument) iRecord;

    if (document.getIdentity().isPersistent() && document.isTrackingChanges()) {
      for (String fieldName : document.getDirtyFields())
        buffer = fieldToStream(buffer, document, fieldName, document.getOriginalValue(fieldName), document.fieldType(fieldName));
    } else
      for (String fieldName : document.fieldNames())
        buffer = fieldToStream(buffer, document, fieldName, document.getOriginalValue(fieldName), document.fieldType(fieldName));

    return buffer;
  }

  @Override
  public String toString() {
    return NAME;
  }

  protected byte[] updateVarContentInSchemaFullSection(final byte[] iSource, final int iOffset, final int iCurrentSize,
      final int iNewSize, final OClass iClass, final int schemaLessSectionOffset, OBinarySerializer<Object> serializer,
      Object iFieldValue) {

    final int delta = iNewSize - iCurrentSize;

    // ALLOCATE MORE SPACE
    final byte[] newBuffer = new byte[iSource.length + delta];

    // COPY DATA BEFORE OFFSET
    System.arraycopy(iSource, 0, newBuffer, 0, iOffset);

    // SERIALIZE NEW VALUE
    serializer.serialize(iFieldValue, newBuffer, iOffset);

    if (iOffset + iNewSize < schemaLessSectionOffset)
      // MOVE VAR CONTENT RIGHT
      System.arraycopy(iSource, iOffset + iCurrentSize, newBuffer, iOffset + iNewSize, iSource.length - iOffset + iCurrentSize);

    // UPDATE VAR-CONTENT OFFSETS
    for (OProperty p : iClass.properties()) {
      if (!p.isFixedSize()) {
        // VAR CONTENT: CHECK IF IT'S IN THE MOVED RANGE
        final int pOffset = p.getPersistentOffset();
        final int varContentOffset = OIntegerSerializer.INSTANCE.deserialize(newBuffer, pOffset);
        if (varContentOffset > iOffset)
          // UPDATE POINTER TO VAR CONTENT OFFSET
          OIntegerSerializer.INSTANCE.serialize(varContentOffset + delta, newBuffer, pOffset);
      }
    }

    return newBuffer;
  }

  protected byte[] updateVarContentInSchemaLessSection(final byte[] iSource, final int iOffset, final int iCurrentSize,
      final int iNewSize, OBinarySerializer<Object> serializer, Object iFieldValue) {
    final int delta = iNewSize - iCurrentSize;

    // ALLOCATE MORE SPACE
    final byte[] newBuffer = new byte[iSource.length + delta];

    // COPY DATA BEFORE OFFSET
    System.arraycopy(iSource, 0, newBuffer, 0, iOffset);

    // SERIALIZE NEW VALUE
    serializer.serialize(iFieldValue, newBuffer, iOffset);

    if (iOffset + iNewSize < newBuffer.length)
      // MOVE VAR CONTENT RIGHT
      System.arraycopy(iSource, iOffset + iCurrentSize, newBuffer, iOffset + iNewSize, iSource.length - iOffset + iCurrentSize);

    return newBuffer;
  }
}
