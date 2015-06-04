/**
 * Copyright (C) 2009-2015 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.fusesource.jmsbenchmark;

import java.util.Collection;
import java.util.List;

public class Support {

    static public <T> String mkString(Collection<T> values, String delimiter) {
        int i = 0;
        StringBuilder s = new StringBuilder();
        for (T e : values) {
            if (i != 0) {
                s.append(delimiter);
            }
            s.append(e.toString());
            i++;
        }
        return s.toString();
    }

    static public <T> T last(List<T> collection) {
        return collection.get(collection.size()-1);
    }

    static public String stripSuffix(String value, String suffix){
        if( value == null || suffix == null ) {
            return value;
        }
        if( value.endsWith(suffix) ) {
            return value.substring(0, value.length() - suffix.length());
        }
        return value;
    }

}
