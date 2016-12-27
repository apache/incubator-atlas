/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.typedef;

import org.apache.commons.collections.CollectionUtils;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasTypesDef {
    private List<AtlasEnumDef>           enumDefs;
    private List<AtlasStructDef>         structDefs;
    private List<AtlasClassificationDef> classificationDefs;
    private List<AtlasEntityDef>         entityDefs;

    public AtlasTypesDef() {
        enumDefs = new ArrayList<>();
        structDefs = new ArrayList<>();
        classificationDefs = new ArrayList<>();
        entityDefs = new ArrayList<>();
    }

    public AtlasTypesDef(List<AtlasEnumDef> enumDefs, List<AtlasStructDef> structDefs,
                         List<AtlasClassificationDef> classificationDefs,
                         List<AtlasEntityDef> entityDefs) {
        this.enumDefs           = enumDefs;
        this.structDefs         = structDefs;
        this.classificationDefs = classificationDefs;
        this.entityDefs         = entityDefs;
    }

    public List<AtlasEnumDef> getEnumDefs() {
        return enumDefs;
    }

    public void setEnumDefs(List<AtlasEnumDef> enumDefs) {
        this.enumDefs = enumDefs;
    }

    public List<AtlasStructDef> getStructDefs() {
        return structDefs;
    }

    public void setStructDefs(List<AtlasStructDef> structDefs) {
        this.structDefs = structDefs;
    }

    public List<AtlasClassificationDef> getClassificationDefs() {
        return classificationDefs;
    }

    public void setClassificationDefs(List<AtlasClassificationDef> classificationDefs) {
        this.classificationDefs = classificationDefs;
    }

    public List<AtlasEntityDef> getEntityDefs() {
        return entityDefs;
    }

    public void setEntityDefs(List<AtlasEntityDef> entityDefs) {
        this.entityDefs = entityDefs;
    }

    @JsonIgnore
    public boolean isEmpty() {
        return CollectionUtils.isEmpty(enumDefs) &&
                CollectionUtils.isEmpty(structDefs) &&
                CollectionUtils.isEmpty(classificationDefs) &&
                CollectionUtils.isEmpty(entityDefs);
    }
}
