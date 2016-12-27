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
package org.apache.atlas.type;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;

import java.util.List;


/**
 * base class that declares interface for all Atlas types.
 */
public abstract class AtlasType {

    private static final Gson GSON =
            new GsonBuilder().setDateFormat(AtlasBaseTypeDef.SERIALIZED_DATE_FORMAT_STR).create();

    private final String       typeName;
    private final TypeCategory typeCategory;

    protected AtlasType(AtlasBaseTypeDef typeDef) {
        this(typeDef.getName(), typeDef.getCategory());
    }

    protected AtlasType(String typeName, TypeCategory typeCategory) {
        this.typeName     = typeName;
        this.typeCategory = typeCategory;
    }

    public void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    public String getTypeName() { return typeName; }

    public TypeCategory getTypeCategory() { return typeCategory; }

    public abstract Object createDefaultValue();

    public abstract boolean isValidValue(Object obj);

    public abstract Object getNormalizedValue(Object obj);

    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = isValidValue(obj);

        if (!ret) {
            messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
        }

        return ret;
    }


    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        return GSON.fromJson(jsonStr, type);
    }
}
