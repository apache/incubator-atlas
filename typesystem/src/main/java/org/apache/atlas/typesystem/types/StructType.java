/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.typesystem.types;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedStruct;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StructType extends AbstractDataType<IStruct> implements IConstructableType<IStruct, ITypedStruct> {

    public final TypeSystem typeSystem;
    public final String name;
    public final FieldMapping fieldMapping;
    public final Map<AttributeInfo, List<String>> infoToNameMap;
    public final int numFields;
    private final TypedStructHandler handler;

    /**
     * Used when creating a StructType, to support recursive Structs.
     */
    protected StructType(TypeSystem typeSystem, String name, int numFields) {
        this.typeSystem = typeSystem;
        this.name = name;
        this.fieldMapping = null;
        infoToNameMap = null;
        this.numFields = numFields;
        this.handler = null;
    }

    protected StructType(TypeSystem typeSystem, String name, AttributeInfo... fields)
    throws AtlasException {
        this.typeSystem = typeSystem;
        this.name = name;
        this.fieldMapping = constructFieldMapping(fields);
        infoToNameMap = TypeUtils.buildAttrInfoToNameMap(this.fieldMapping);
        this.numFields = this.fieldMapping.fields.size();
        this.handler = new TypedStructHandler(this);
    }

    public FieldMapping fieldMapping() {
        return fieldMapping;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Validate that current definition can be updated with the new definition
     * @param newType
     * @return true if the current definition can be updated with the new definition, else false
     */
    @Override
    public void validateUpdate(IDataType newType) throws TypeUpdateException {
        super.validateUpdate(newType);

        StructType newStructType = (StructType) newType;
        try {
            TypeUtils.validateUpdate(fieldMapping, newStructType.fieldMapping);
        } catch (TypeUpdateException e) {
            throw new TypeUpdateException(newType, e);
        }
    }

    protected FieldMapping constructFieldMapping(AttributeInfo... fields)
    throws AtlasException {

        Map<String, AttributeInfo> fieldsMap = new LinkedHashMap<String, AttributeInfo>();
        Map<String, Integer> fieldPos = new HashMap<String, Integer>();
        Map<String, Integer> fieldNullPos = new HashMap<String, Integer>();
        int numBools = 0;
        int numBytes = 0;
        int numShorts = 0;
        int numInts = 0;
        int numLongs = 0;
        int numFloats = 0;
        int numDoubles = 0;
        int numBigInts = 0;
        int numBigDecimals = 0;
        int numDates = 0;
        int numStrings = 0;
        int numArrays = 0;
        int numMaps = 0;
        int numStructs = 0;
        int numReferenceables = 0;

        for (AttributeInfo i : fields) {
            if (fieldsMap.containsKey(i.name)) {
                throw new AtlasException(
                        String.format("Struct defintion cannot contain multiple fields with the same " + "name %s",
                                i.name));
            }
            fieldsMap.put(i.name, i);
            fieldNullPos.put(i.name, fieldNullPos.size());
            if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
                fieldPos.put(i.name, numBools);
                numBools++;
            } else if (i.dataType() == DataTypes.BYTE_TYPE) {
                fieldPos.put(i.name, numBytes);
                numBytes++;
            } else if (i.dataType() == DataTypes.SHORT_TYPE) {
                fieldPos.put(i.name, numShorts);
                numShorts++;
            } else if (i.dataType() == DataTypes.INT_TYPE) {
                fieldPos.put(i.name, numInts);
                numInts++;
            } else if (i.dataType() == DataTypes.LONG_TYPE) {
                fieldPos.put(i.name, numLongs);
                numLongs++;
            } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
                fieldPos.put(i.name, numFloats);
                numFloats++;
            } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
                fieldPos.put(i.name, numDoubles);
                numDoubles++;
            } else if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
                fieldPos.put(i.name, numBigInts);
                numBigInts++;
            } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                fieldPos.put(i.name, numBigDecimals);
                numBigDecimals++;
            } else if (i.dataType() == DataTypes.DATE_TYPE) {
                fieldPos.put(i.name, numDates);
                numDates++;
            } else if (i.dataType() == DataTypes.STRING_TYPE) {
                fieldPos.put(i.name, numStrings);
                numStrings++;
            } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
                fieldPos.put(i.name, numInts);
                numInts++;
            } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                fieldPos.put(i.name, numArrays);
                numArrays++;
            } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
                fieldPos.put(i.name, numMaps);
                numMaps++;
            } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT
                    || i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                fieldPos.put(i.name, numStructs);
                numStructs++;
            } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                fieldPos.put(i.name, numReferenceables);
                numReferenceables++;
            } else {
                throw new AtlasException(String.format("Unknown datatype %s", i.dataType()));
            }
        }

        return new FieldMapping(fieldsMap, fieldPos, fieldNullPos, numBools, numBytes, numShorts, numInts, numLongs,
                numFloats, numDoubles, numBigInts, numBigDecimals, numDates, numStrings, numArrays, numMaps, numStructs,
                numReferenceables);
    }


    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.STRUCT;
    }

    @Override
    public ITypedStruct convert(Object val, Multiplicity m) throws AtlasException {
        return handler.convert(val, m);
    }

    public ITypedStruct createInstance() {
        return handler.createInstance();
    }

    @Override
    public void output(IStruct s, Appendable buf, String prefix) throws AtlasException {
        handler.output(s, buf, prefix);
    }

    @Override
    public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
        if( !(val instanceof  ITypedStruct)) {
            throw new IllegalArgumentException("Unexpected value type " + val.getClass().getSimpleName() + ". Expected instance of ITypedStruct");
        }
        digester.update(getName().getBytes(Charset.forName("UTF-8")));

        if(fieldMapping.fields != null && val != null) {
            IStruct typedValue = (IStruct) val;
            for (AttributeInfo aInfo : fieldMapping.fields.values()) {
                Object attrVal = typedValue.get(aInfo.name);
                if(attrVal != null) {
                    aInfo.dataType().updateSignatureHash(digester, attrVal);
                }
            }
        }
    }

    public List<String> getNames(AttributeInfo info) {
        return infoToNameMap.get(info);
    }
}
