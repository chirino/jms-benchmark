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

public class DataSample {

    public final long time;
    public final long produced;
    public final long consumed;
    public final long errors;
    public final long maxLatency;

    public DataSample(long time, long produced, long consumed, long errors, long maxLatency){
        this.time = time;
        this.produced = produced;
        this.consumed = consumed;
        this.errors = errors;
        this.maxLatency = maxLatency;
    }

    @Override
    public String toString() {
        return "DataSample{" +
                "consumed=" + consumed +
                ", time=" + time +
                ", produced=" + produced +
                ", errors=" + errors +
                ", maxLatency=" + maxLatency +
                '}';
    }

    public long getConsumed() {
        return consumed;
    }

    public long getErrors() {
        return errors;
    }

    public long getMaxLatency() {
        return maxLatency;
    }

    public long getProduced() {
        return produced;
    }

    public long getTime() {
        return time;
    }
}
