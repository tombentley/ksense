/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tombentley.ksense.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class Pool<K, V> {

    private final Supplier<V> supplier;
    private final Map<K, V> map;

    public Pool(Supplier<V> supplier) {
        this.supplier = supplier;
        map = new HashMap<>();
    }

    public V get(K key) {
        return map.computeIfAbsent(key, k -> supplier.get());
    }

    public void remove(K key) {
        map.remove(key);
    }

    public void clear() {
        map.clear();
    }
}
