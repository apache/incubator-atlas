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
package org.apache.atlas.typesystem.types.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;

import com.google.inject.Singleton;

/**
 * Caches the types in-memory within the same process space.
 */
@Singleton
@SuppressWarnings("rawtypes")
public class DefaultTypeCacheProvider implements ITypeCacheProvider {

    private Map<String, IDataType> types_ = new ConcurrentHashMap<>();

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#has(java.lang
     * .String)
     */
    @Override
    public boolean has(String typeName) throws AtlasException {

        return types_.containsKey(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#has(org.
     * apache.atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public boolean has(TypeCategory typeCategory, String typeName)
        throws AtlasException {

        assertValidTypeCategory(typeCategory);
        return has(typeName);
    }

    private void assertValidTypeCategory(TypeCategory typeCategory) throws
        AtlasException {

        // there might no need of 'typeCategory' in this implementation for
        // certain API, but for a distributed cache, it might help for the
        // implementers to partition the types per their category
        // while persisting so that look can be efficient

        if (typeCategory == null) {
            throw new AtlasException("Category of the types to be filtered is null.");
        }

        boolean validTypeCategory = typeCategory.equals(TypeCategory.CLASS) ||
            typeCategory.equals(TypeCategory.TRAIT) ||
            typeCategory.equals(TypeCategory.ENUM) ||
            typeCategory.equals(TypeCategory.STRUCT);

        if (!validTypeCategory) {
            throw new AtlasException("Category of the types should be one of CLASS "
                + "| TRAIT | ENUM | STRUCT.");
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#get(java.lang
     * .String)
     */
    @Override
    public IDataType get(String typeName) throws AtlasException {

        return types_.get(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#get(org.apache.
     * atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public IDataType get(TypeCategory typeCategory, String typeName) throws AtlasException {

        assertValidTypeCategory(typeCategory);
        return get(typeName);
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#getNames(org
     * .apache.atlas.typesystem.types.DataTypes.TypeCategory)
     */
    @Override
    public Collection<String> getTypeNames(TypeCategory typeCategory) throws AtlasException {

        assertValidTypeCategory(typeCategory);

        List<String> typeNames = new ArrayList<>();
        for (Entry<String, IDataType> typeEntry : types_.entrySet()) {
            String name = typeEntry.getKey();
            IDataType type = typeEntry.getValue();

            if (type.getTypeCategory().equals(typeCategory)) {
                typeNames.add(name);
            }
        }
        return typeNames;
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#getAllNames()
     */
    @Override
    public Collection<String> getAllTypeNames() throws AtlasException {

        return types_.keySet();
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#put(org.apache
     * .atlas.typesystem.types.IDataType)
     */
    @Override
    public void put(IDataType type) throws AtlasException {

        assertValidType(type);
        types_.put(type.getName(), type);
    }

    private void assertValidType(IDataType type) throws
        AtlasException {

        if (type == null) {
            throw new AtlasException("type is null.");
        }

        boolean validTypeCategory = (type instanceof ClassType) ||
            (type instanceof TraitType) ||
            (type instanceof EnumType) ||
            (type instanceof StructType);

        if (!validTypeCategory) {
            throw new AtlasException("Category of the types should be one of ClassType | "
                + "TraitType | EnumType | StructType.");
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#putAll(java
     * .util.Collection)
     */
    @Override
    public void putAll(Collection<IDataType> types) throws AtlasException {

        for (IDataType type : types) {
            assertValidType(type);
            types_.put(type.getName(), type);
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#remove(java
     * .lang.String)
     */
    @Override
    public void remove(String typeName) throws AtlasException {

        types_.remove(typeName);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#remove(org.
     * apache.atlas.typesystem.types.DataTypes.TypeCategory, java.lang.String)
     */
    @Override
    public void remove(TypeCategory typeCategory, String typeName)
            throws AtlasException {

        assertValidTypeCategory(typeCategory);
        remove(typeName);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.atlas.typesystem.types.cache.ITypeCacheProvider#clear()
     */
    @Override
    public void clear() {

        types_.clear();
    }
}
