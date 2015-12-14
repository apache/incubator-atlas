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

package org.apache.atlas.typesystem.types.utils;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import scala.collection.JavaConversions;

/**
 * Types utilities class.
 */
public class TypesUtil {

    private TypesUtil() {
    }

    public static AttributeDefinition createOptionalAttrDef(String name, IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(), Multiplicity.OPTIONAL, false, null);
    }

    public static AttributeDefinition createOptionalAttrDef(String name, String dataType) {
        return new AttributeDefinition(name, dataType, Multiplicity.OPTIONAL, false, null);
    }

    public static AttributeDefinition createRequiredAttrDef(String name, String dataType) {
        return new AttributeDefinition(name, dataType, Multiplicity.REQUIRED, false, null);
    }

    public static AttributeDefinition createUniqueRequiredAttrDef(String name, IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(), Multiplicity.REQUIRED, false, true, true, null);
    }

    public static AttributeDefinition createRequiredAttrDef(String name, IDataType dataType) {
        return new AttributeDefinition(name, dataType.getName(), Multiplicity.REQUIRED, false, null);
    }

    public static EnumTypeDefinition createEnumTypeDef(String name, EnumValue... enumValues) {
        return new EnumTypeDefinition(name, enumValues);
    }

    public static HierarchicalTypeDefinition<TraitType> createTraitTypeDef(String name,
            ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition<>(TraitType.class, name, superTypes, attrDefs);
    }

    public static StructTypeDefinition createStructTypeDef(String name, AttributeDefinition... attrDefs) {
        return new StructTypeDefinition(name, attrDefs);
    }

    public static HierarchicalTypeDefinition<ClassType> createClassTypeDef(String name,
            ImmutableList<String> superTypes, AttributeDefinition... attrDefs) {
        return new HierarchicalTypeDefinition<>(ClassType.class, name, superTypes, attrDefs);
    }

    public static TypesDef getTypesDef(ImmutableList<EnumTypeDefinition> enums,
                                       ImmutableList<StructTypeDefinition> structs, ImmutableList<HierarchicalTypeDefinition<TraitType>> traits,
                                       ImmutableList<HierarchicalTypeDefinition<ClassType>> classes) {
        return new TypesDef(JavaConversions.asScalaBuffer(enums), JavaConversions.asScalaBuffer(structs),
                JavaConversions.asScalaBuffer(traits), JavaConversions.asScalaBuffer(classes));
    }
}
