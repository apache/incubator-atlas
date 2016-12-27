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

import com.google.common.collect.ImmutableSortedMap;

import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

abstract class AbstractDataType<T> implements IDataType<T> {

    public final String name;
    public final String description;
    public final String version;

    public AbstractDataType(String name, String description) {

        super();
        this.name = name;
        this.description = description;
        this.version = AtlasConstants.DEFAULT_TYPE_VERSION;
    }

    public AbstractDataType(String name, String description, String version) {

        super();
        this.name = name;
        this.description = description;
        this.version = version;
    }

    protected T convertNull(Multiplicity m) throws AtlasException {
        if (!m.nullAllowed()) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    @Override
    public void output(T val, Appendable buf, String prefix, Set<T> inProcess) throws AtlasException {
        final String strValue;

        if (val == null) {
            strValue = "<null>";
        } else if (val instanceof Map) {
            ImmutableSortedMap immutableSortedMap = ImmutableSortedMap.copyOf((Map) val);
            strValue = immutableSortedMap.toString();
        } else {
            strValue = val.toString();
        }

        TypeUtils.outputVal(strValue, buf, prefix);
    }

    @Override
    public void output(Appendable buf, Set<String> typesInProcess) throws AtlasException {

        try {
            buf.append(toString());
        } catch (IOException e) {
            throw new AtlasException(e);
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "{name=" + name + ", description=" + description + "}";
    }

    /**
     * Validate that current definition can be updated with the new definition
     * @param newType
     */
    @Override
    public void validateUpdate(IDataType newType) throws TypeUpdateException {
        if (!getName().equals(newType.getName()) || !getClass().getName().equals(newType.getClass().getName())) {
            throw new TypeUpdateException(newType);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getVersion() {
        return version;
    }
}

