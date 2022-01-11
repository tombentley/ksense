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

import java.util.Arrays;

import static java.lang.Math.sqrt;

/**
 * A buffer of some bounded number of (x,y) pairs.
 * Pairs are added via {@link #add(double, double)}.
 * A simple linear regression over the added pairs can be done via {@link #leastSq()}.
 */
class XYBuffer {

    private static final double NAN = Double.NaN;
    private int head; // the next insertion point
    private int size;
    private final double[] x;
    private final double[] y;

    public XYBuffer(int maxSize) {
        if (maxSize <=0) {
            throw new IllegalArgumentException();
        }
        this.head = 0;
        this.size = 0;
        this.x = new double[maxSize];
        this.y = new double[maxSize];
    }

    /**
     * @return The current number of points in the buffer.
     */
    public int size() {
        return size;
    }

    /**
     * @return The maximum possible number of points in the buffer.
     */
    public int maxSize() {
        return x.length;
    }

    /**
     * Appends a pair of data points to the buffer
     * @param x The first element of the pair
     * @param y The second element of the pair
     */
    public void add(double x, double y) {
        this.x[head] = x;
        this.y[head] = y;
        if (this.size < this.x.length) {
            this.size = this.head + 1;
        }
        this.head = (this.head + 1) % (this.x.length);
    }

    /**
     * @return The most recently added x value, or NaN.
     * Whether this is the largest or smallest value of x depends on whether x is monotonic and whether x observations are added in order.
     */
    public double newestX() {
        return this.head == 0 ? NAN : this.x[this.head - 1];
    }

    /**
     * @return The most recently added y value, or NaN.
     */
    public double newestY() {
        return this.head == 0 ? NAN : this.y[this.head - 1];
    }

    /**
     * Clears the contents of the buffer.
     */
    public void clear() {
        this.head = this.size = 0;
    }

    @Override
    public String toString() {
        var sb = new StringBuffer("XYBuffer(");
        if (size > 0) {
            for (int i = 0; i < (size); i++) {
                sb.append('(').append(x[i]).append(", ").append(y[i]).append(')');
                if (i < size - 1) {
                    sb.append(", ");
                }
            }
        } else {
            sb.append(")");
        }
        return sb.toString();
    }

    private static double sum(double[] v, int size) {
        if (size > 0) {
            double r = 0;
            for (int i = 0; i < (size); i++) {
                r += v[i];
            }
            return r;
        } else {
            return NAN;
        }
    }

    private static double sumprod(double[] u, double[] v, int size) {
        if (size > 0) {
            double r = 0;
            for (int i = 0; i < (size); i++) {
                r += u[i] * v[i];
            }
            return r;
        } else {
            return NAN;
        }
    }

    /**
     * Simple linear regression over at-most <i>N</i> points.
     * @see <a href="https://en.wikipedia.org/wiki/Simple_linear_regression">https://en.wikipedia.org/wiki/Simple_linear_regression</a>
     */
    public BestFit leastSq() {
        double sx = sum(this.x, this.size);
        double sxx = sumprod(this.x, this.x, this.size);
        double sy = sum(this.y, this.size);
        double syy = sumprod(this.y, this.y, this.size);
        double sxy = sumprod(this.x, this.y, this.size);
        double beta = (this.size * sxy - sx * sy) / (this.size  * sxx - sx * sx);
        double alpha = sy / this.size - beta * sx / this.size;
        double se2 = 1.0/(this.size * (this.size - 2)) * (this.size * syy - sy*sy - beta * beta * (this.size * sxx - sx * sx));
        double sbeta2 = (this.size * se2) / (this.size * sxx - sx * sx);
        double salpha2 = sbeta2 * sxx / this.size;

        if (size <= 1) {
            return new BestFit(NAN, NAN, NAN, NAN);
        } else if (size <= 2) {
            return new BestFit(alpha, NAN, beta, NAN);
        } else {
            double t = student0975(this.size - 2);
            return new BestFit(alpha, t * sqrt(salpha2), beta, t * sqrt(sbeta2));
        }

    }

    /**
     * A line of best fit, as the result of a simple linear regression.
     * <pre>
     * y = alpha + beta * x;
     * </pre>
     * See {@link #leastSq()}.
     */
    public static class BestFit {
        private final double alpha;
        private final double deltaAlpha;
        private final double beta;
        private final double deltaBeta;

        public BestFit(double alpha, double deltaAlpha, double beta, double deltaBeta) {
            this.alpha = alpha;
            this.deltaAlpha = deltaAlpha;
            this.beta = beta;
            this.deltaBeta = deltaBeta;
        }

        /**
         * @return The y-intercept of the line of best fit.
         */
        public double alpha() {
            return alpha;
        }

        /**
         * @return A 97.5%-confidence tolerance interval on {@link #alpha()}.
         */
        public double alphaDelta() {
            return deltaAlpha;
        }

        /**
         * @return The gradient of the line of best fit.
         */
        public double beta() {
            return beta;
        }

        /**
         * @return A 97.5%-confidence tolerance interval on {@link #beta()}.
         */
        public double betaDelta() {
            return deltaBeta;
        }

        @Override
        public String toString() {
            return "BestFit(" +
                    "alpha=" + alpha + " ±" + deltaAlpha +
                    ", beta=" + beta + " ±" + deltaBeta +
                    ')';
        }
    }

    private double student0975(int dof) {
        double[] x = {
                12.706,//1
                4.303,
                3.182,
                2.776,
                2.571,
                2.447,
                2.365,
                2.306,
                2.262,
                2.228,
                2.201,
                2.179,
                2.160,
                2.145,
                2.131,
                2.120,
                2.110,
                2.101,
                2.093,
                2.086,
                2.080,
                2.074,
                2.069,
                2.064,
                2.060,
                2.056,
                2.052,
                2.048,
                2.045,//29
                2.042,//30
                2.021,//40
                2.000,//60
                1.990,//80
                1.984,//100
                1.962,//1000
                1.960//inf
            };
            // TODO dof > 30
            return x[dof - 1];
        }
}
