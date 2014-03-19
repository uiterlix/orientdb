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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;

import java.util.Set;

/**
 * Serializer for {@link Long} type.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OEmbeddedSetSerializer implements OBinarySerializer<Set> {
  public static final byte              ID        = 10;
  /**
   * size of long value in bytes
   */
  private static final OBinaryConverter CONVERTER = OBinaryConverterFactory.getConverter();
  public static OEmbeddedSetSerializer  INSTANCE  = new OEmbeddedSetSerializer();

  public int getObjectSize(Set object, Object... hints) {
    return 0;
  }

  public void serialize(Set object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serialize(object.size(), stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    for (Object o : object) {

    }
  }

  public Set deserialize(byte[] stream, int startPosition) {
    return null;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return 0;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return 0;
  }

  public void serializeNative(Set object, byte[] stream, int startPosition, Object... hints) {
  }

  public Set deserializeNative(byte[] stream, int startPosition) {
    return null;
  }

  @Override
  public void serializeInDirectMemory(Set object, ODirectMemoryPointer pointer, long offset, Object... hints) {

  }

  @Override
  public Set deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return null;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return 0;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public Set preprocess(Set value, Object... hints) {
    return value;
  }
}
