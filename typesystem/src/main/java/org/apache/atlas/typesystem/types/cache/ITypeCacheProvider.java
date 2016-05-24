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

import java.util.Collection;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;

/**
 * The types are cached to allow faster lookup when type info is needed during
 * creation/updation of entities, DSL query translation/execution.
 * Implementations of this can chose to plugin a distributed cache provider
 * or an in-memory cache synched across nodes in an Altas cluster. <br>
 * <br>
 * Type entries in the cache can be one of ... <br>
 * {@link org.apache.atlas.typesystem.types.ClassType} <br>
 * {@link org.apache.atlas.typesystem.types.TraitType} <br>
 * {@link org.apache.atlas.typesystem.types.StructType} <br>
 * {@link org.apache.atlas.typesystem.types.EnumType}
 */
@SuppressWarnings("rawtypes")
public interface ITypeCacheProvider {

    /**
     * @param typeName
     * @return true if the type exists in cache, false otherwise.
     * @throws AtlasException
     */
    boolean has(String typeName) throws AtlasException;

    /**
     * @param typeCategory Non-null category of type. The category can be one of
     * TypeCategory.CLASS | TypeCategory.TRAIT | TypeCategory.STRUCT | TypeCategory.ENUM.
     * @param typeName
     * @return true if the type of given category exists in cache, false otherwise.
     * @throws AtlasException
     */
    boolean has(DataTypes.TypeCategory typeCategory, String typeName) throws AtlasException;

    /**
     * @param name The name of the type.
     * @return returns non-null type if cached, otherwise null
     * @throws AtlasException
     */
    public IDataType get(String typeName) throws AtlasException;

    /**
     * @param typeCategory Non-null category of type. The category can be one of
     * TypeCategory.CLASS | TypeCategory.TRAIT | TypeCategory.STRUCT | TypeCategory.ENUM.
     * @param typeName
     * @return returns non-null type (of the specified category) if cached, otherwise null
     * @throws AtlasException
     */
    public IDataType get(DataTypes.TypeCategory typeCategory, String typeName) throws AtlasException;

    /**
     * @param typeCategory The category of types to filter the returned types. Cannot be null.
     * The category can be one of TypeCategory.CLASS | TypeCategory.TRAIT |
     * TypeCategory.STRUCT | TypeCategory.ENUM.
     * @return
     * @throws AtlasException
     */
    Collection<String> getTypeNames(DataTypes.TypeCategory typeCategory) throws AtlasException;

    /**
     * This is a convenience API to get the names of all types.
     *
     * @see ITypeCacheProvider#getTypeNames(org.apache.atlas.typesystem.types.DataTypes.TypeCategory)
     * @return
     * @throws AtlasException
     */
    Collection<String> getAllTypeNames() throws AtlasException;

    /**
     * @param type The type to be added to the cache. The type should not be
     * null, otherwise throws NullPointerException. <br>
     * Type entries in the cache can be one of ... <br>
     * {@link org.apache.atlas.typesystem.types.ClassType} <br>
     * {@link org.apache.atlas.typesystem.types.TraitType} <br>
     * {@link org.apache.atlas.typesystem.types.StructType} <br>
     * {@link org.apache.atlas.typesystem.types.EnumType}
     * @throws AtlasException
     */
    void put(IDataType type) throws AtlasException;

    /**
     * @param types The types to be added to the cache. The type should not be
     * null, otherwise throws NullPointerException. <br>
     * Type entries in the cache can be one of ... <br>
     * {@link org.apache.atlas.typesystem.types.ClassType} <br>
     * {@link org.apache.atlas.typesystem.types.TraitType} <br>
     * {@link org.apache.atlas.typesystem.types.StructType} <br>
     * {@link org.apache.atlas.typesystem.types.EnumType}
     * @throws AtlasException
     */
    void putAll(Collection<IDataType> types) throws AtlasException;

    /**
     * @param typeName Name of the type to be removed from the cache. If type
     * exists, it will be removed, otherwise does nothing.
     * @throws AtlasException
     */
    void remove(String typeName) throws AtlasException;

    /**
     * @param typeCategory Non-null category of type. The category can be one of
     * TypeCategory.CLASS | TypeCategory.TRAIT | TypeCategory.STRUCT | TypeCategory.ENUM.
     * @param typeName Name of the type to be removed from the cache. If type
     * exists, it will be removed, otherwise does nothing.
     * @throws AtlasException
     */
    void remove(DataTypes.TypeCategory typeCategory, String typeName) throws AtlasException;

    /**
     * Clear the type cache
     *
     */
    void clear();
}
