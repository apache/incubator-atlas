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
package org.apache.atlas.model.instance;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import org.apache.atlas.model.PList;
import org.apache.atlas.model.SearchFilter.SortType;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;


/**
 * An instance of a classification; it doesn't have an identity, this object exists only when associated with an entity.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasClassification extends AtlasStruct implements Serializable {
    private static final long serialVersionUID = 1L;

    public AtlasClassification() {
        this(null, null);
    }

    public AtlasClassification(String typeName) {
        this(typeName, null);
    }

    public AtlasClassification(String typeName, Map<String, Object> attributes) {
        super(typeName, attributes);
    }

    public AtlasClassification(String typeName, String attrName, Object attrValue) {
        super(typeName, attrName, attrValue);
    }

    public AtlasClassification(AtlasClassification other) {
        if (other != null) {
            setTypeName(other.getTypeName());
            setAttributes(other.getAttributes());
        }
    }

    /**
     * REST serialization friendly list.
     */
    @JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.PROPERTY)
    @XmlSeeAlso(AtlasClassification.class)
    public static class AtlasClassifications extends PList<AtlasClassification> {
        private static final long serialVersionUID = 1L;

        public AtlasClassifications() {
            super();
        }

        public AtlasClassifications(List<AtlasClassification> list) {
            super(list);
        }

        public AtlasClassifications(List list, long startIndex, int pageSize, long totalCount,
                                    SortType sortType, String sortBy) {
            super(list, startIndex, pageSize, totalCount, sortType, sortBy);
        }
    }
}
