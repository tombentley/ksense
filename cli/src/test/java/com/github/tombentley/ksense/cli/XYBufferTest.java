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

import com.github.tombentley.ksense.cli.XYBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
class XYBufferTest {

    @Test
    public void test1() {
        withSize(15);
        withSize(16);
    }

    private void withSize(int size) {
        // See https://en.wikipedia.org/wiki/Simple_linear_regression#Numerical_example
        var rb = new XYBuffer(size);
        assertEquals(Double.NaN, rb.newestX());
        assertEquals(Double.NaN, rb.newestY());
        rb.add(1.47, 52.21);
        assertEquals(1.47, rb.newestX());
        assertEquals(52.21, rb.newestY());
        rb.add(1.50, 53.12);
        rb.add(1.52, 54.48);
        rb.add(1.55, 55.84);
        rb.add(1.57, 57.20);
        rb.add(1.60, 58.57);
        rb.add(1.63, 59.93);
        rb.add(1.65, 61.29);
        rb.add(1.68, 63.11);
        rb.add(1.70, 64.47);
        rb.add(1.73, 66.28);
        rb.add(1.75, 68.10);
        rb.add(1.78, 69.92);
        rb.add(1.80, 72.19);
        rb.add(1.83, 74.46);
        var result  = rb.leastSq();
        assertEquals(-39.06195591883866, result.alpha());
        assertEquals(6.346082305124089, result.alphaDelta());
        assertEquals(61.272186542107434, result.beta());
        assertEquals(3.8359931447899345, result.betaDelta());
    }

    @Test
    public void truncated() {
        var rb1 = new XYBuffer(14);
        rb1.add(1.47, 52.21);
        rb1.add(1.50, 53.12);
        rb1.add(1.52, 54.48);
        rb1.add(1.55, 55.84);
        rb1.add(1.57, 57.20);
        rb1.add(1.60, 58.57);
        rb1.add(1.63, 59.93);
        rb1.add(1.65, 61.29);
        rb1.add(1.68, 63.11);
        rb1.add(1.70, 64.47);
        rb1.add(1.73, 66.28);
        rb1.add(1.75, 68.10);
        rb1.add(1.78, 69.92);
        rb1.add(1.80, 72.19);
        rb1.add(1.83, 74.46);
        var result1  = rb1.leastSq();

        var rb2 = new XYBuffer(14);
        //rb2.add(1.47, 52.21);
        rb2.add(1.50, 53.12);
        rb2.add(1.52, 54.48);
        rb2.add(1.55, 55.84);
        rb2.add(1.57, 57.20);
        rb2.add(1.60, 58.57);
        rb2.add(1.63, 59.93);
        rb2.add(1.65, 61.29);
        rb2.add(1.68, 63.11);
        rb2.add(1.70, 64.47);
        rb2.add(1.73, 66.28);
        rb2.add(1.75, 68.10);
        rb2.add(1.78, 69.92);
        rb2.add(1.80, 72.19);
        rb2.add(1.83, 74.46);
        var result2  = rb2.leastSq();

        assertEquals(result1.alpha(), result2.alpha(), 1E-8);
        assertEquals(result1.alphaDelta(), result2.alphaDelta(), 1E-8);
        assertEquals(result1.beta(), result2.beta(), 1E-8);
        assertEquals(result1.betaDelta(), result2.betaDelta(), 1E-8);
    }

}