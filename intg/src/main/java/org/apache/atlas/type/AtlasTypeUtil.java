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

import com.google.common.collect.ImmutableSet;

import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef.Cardinality;
import org.apache.atlas.model.typedef.AtlasTypeDefHeader;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;
import java.util.Set;
import java.util.Arrays;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX;

/**
 * Utility methods for AtlasType/AtlasTypeDef.
 */
public class AtlasTypeUtil {
    private static final Set<String> ATLAS_BUILTIN_TYPENAMES = new HashSet<>();

    static {
        Collections.addAll(ATLAS_BUILTIN_TYPENAMES, AtlasBaseTypeDef.ATLAS_BUILTIN_TYPES);
    }

    public static Set<String> getReferencedTypeNames(String typeName) {
        Set<String> ret = new HashSet<>();

        getReferencedTypeNames(typeName, ret);

        return ret;
    }

    public static boolean isBuiltInType(String typeName) {
        return ATLAS_BUILTIN_TYPENAMES.contains(typeName);
    }

    public static boolean isArrayType(String typeName) {
        return StringUtils.startsWith(typeName, ATLAS_TYPE_ARRAY_PREFIX)
            && StringUtils.endsWith(typeName, ATLAS_TYPE_ARRAY_SUFFIX);
    }

    public static boolean isMapType(String typeName) {
        return StringUtils.startsWith(typeName, ATLAS_TYPE_MAP_PREFIX)
            && StringUtils.endsWith(typeName, ATLAS_TYPE_MAP_SUFFIX);
    }


    public static String getStringValue(Map map, Object key) {
        Object ret = map != null ? map.get(key) : null;

        return ret != null ? ret.toString() : null;
    }

    private static void getReferencedTypeNames(String typeName, Set<String> referencedTypeNames) {
        if (StringUtils.isNotBlank(typeName) && !referencedTypeNames.contains(typeName)) {
            if (typeName.startsWith(ATLAS_TYPE_ARRAY_PREFIX) && typeName.endsWith(ATLAS_TYPE_ARRAY_SUFFIX)) {
                int    startIdx        = ATLAS_TYPE_ARRAY_PREFIX.length();
                int    endIdx          = typeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                String elementTypeName = typeName.substring(startIdx, endIdx);

                getReferencedTypeNames(elementTypeName, referencedTypeNames);
            } else if (typeName.startsWith(ATLAS_TYPE_MAP_PREFIX) && typeName.endsWith(ATLAS_TYPE_MAP_SUFFIX)) {
                int      startIdx      = ATLAS_TYPE_MAP_PREFIX.length();
                int      endIdx        = typeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                String[] keyValueTypes = typeName.substring(startIdx, endIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                String   keyTypeName   = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                String   valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;

                getReferencedTypeNames(keyTypeName, referencedTypeNames);
                getReferencedTypeNames(valueTypeName, referencedTypeNames);
            } else {
                referencedTypeNames.add(typeName);
            }
        }

    }

    public static AtlasAttributeDef createOptionalAttrDef(String name, AtlasType dataType) {
        return new AtlasAttributeDef(name, dataType.getTypeName(), true,
            Cardinality.SINGLE, 0, 1,
            false, false,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasAttributeDef createOptionalAttrDef(String name, String dataType) {
        return new AtlasAttributeDef(name, dataType, true,
            Cardinality.SINGLE, 0, 1,
            false, false,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasAttributeDef createRequiredAttrDef(String name, String dataType) {
        return new AtlasAttributeDef(name, dataType, false,
            Cardinality.SINGLE, 1, 1,
            false, true,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasAttributeDef createUniqueRequiredAttrDef(String name, AtlasType dataType) {
        return new AtlasAttributeDef(name, dataType.getTypeName(), false,
            Cardinality.SINGLE, 1, 1,
            true, true,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasAttributeDef createUniqueRequiredAttrDef(String name, String typeName) {
        return new AtlasAttributeDef(name, typeName, false,
            Cardinality.SINGLE, 1, 1,
            true, true,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasAttributeDef createRequiredAttrDef(String name, AtlasType dataType) {
        return new AtlasAttributeDef(name, dataType.getTypeName(), false,
            Cardinality.SINGLE, 1, 1,
            false, true,
            Collections.<AtlasStructDef.AtlasConstraintDef>emptyList());
    }

    public static AtlasEnumDef createEnumTypeDef(String name, String description, AtlasEnumElementDef... enumValues) {
        return new AtlasEnumDef(name, description, "1.0", Arrays.asList(enumValues));
    }

    public static AtlasClassificationDef createTraitTypeDef(String name, ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return createTraitTypeDef(name, null, superTypes, attrDefs);
    }

    public static AtlasClassificationDef createTraitTypeDef(String name, String description, ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return createTraitTypeDef(name, description, "1.0", superTypes, attrDefs);
    }

    public static AtlasClassificationDef createTraitTypeDef(String name, String description, String version, ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return new AtlasClassificationDef(name, description, "1.0", Arrays.asList(attrDefs), superTypes);
    }

    public static AtlasStructDef createStructTypeDef(String name, AtlasAttributeDef... attrDefs) {
        return createStructTypeDef(name, null, attrDefs);
    }

    public static AtlasStructDef createStructTypeDef(String name, String description, AtlasAttributeDef... attrDefs) {
        return new AtlasStructDef(name, description, "1.0", Arrays.asList(attrDefs));
    }

    public static AtlasEntityDef createClassTypeDef(String name,
        ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return createClassTypeDef(name, null, "1.0", superTypes, attrDefs);
    }

    public static AtlasEntityDef createClassTypeDef(String name, String description,
        ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return createClassTypeDef(name, description, "1.0", superTypes, attrDefs);
    }

    public static AtlasEntityDef createClassTypeDef(String name, String description, String version,
        ImmutableSet<String> superTypes, AtlasAttributeDef... attrDefs) {
        return new AtlasEntityDef(name, description, "1.0", Arrays.asList(attrDefs), superTypes);
    }

    public static AtlasTypesDef getTypesDef(List<AtlasEnumDef> enums,
        List<AtlasStructDef> structs,
        List<AtlasClassificationDef> traits,
        List<AtlasEntityDef> classes) {
        return new AtlasTypesDef(enums, structs, traits, classes);
    }

    public static List<AtlasTypeDefHeader> toTypeDefHeader(AtlasTypesDef typesDef) {
        List<AtlasTypeDefHeader> headerList = new LinkedList<>();
        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
                headerList.add(new AtlasTypeDefHeader(enumDef));
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                headerList.add(new AtlasTypeDefHeader(structDef));
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef classificationDef : typesDef.getClassificationDefs()) {
                headerList.add(new AtlasTypeDefHeader(classificationDef));
            }
        }
        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                headerList.add(new AtlasTypeDefHeader(entityDef));
            }
        }

        return headerList;
    }
}