// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.application;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomGenerator {

    private static final Logger log =
            LoggerFactory.getLogger(RandomGenerator.class);

    private final static int standardLower = 7;
    private final static int standardUpper = 21;

    public static String[] randomStrings(int number) {
        return randomStrings(number, standardLower, standardUpper);
    }

    public static String[] randomStrings(int number, int lowerLen, int upperLen) {
        String[] ret = new String[number];
        for (int i = 0; i < number; i++)
            ret[i] = randomString(lowerLen, upperLen);
        return ret;
    }

    public static String randomString() {
        return randomString(standardLower, standardUpper);
    }

    public static String randomString(int lowerLen, int upperLen) {
        Preconditions.checkState(lowerLen > 0 && upperLen >= lowerLen);
        int length = randomInt(lowerLen, upperLen);
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < length; i++) {
            s.append((char) randomInt(97, 120));
        }
        return s.toString();
    }

    /**
     * Generate a pseudorandom number using Math.random().
     *
     * @param lower minimum returned random number, inclusive
     * @param upper maximum returned random number, exclusive
     * @return the generated pseudorandom
     */
    public static int randomInt(int lower, int upper) {
        Preconditions.checkState(upper > lower);
        int interval = upper - lower;
        // Generate a random int on [lower, upper)
        double rand = Math.floor(Math.random() * interval) + lower;
        // Shouldn't happen
        if (rand >= upper)
            rand = upper - 1;
        // Cast and return
        return (int) rand;
    }

    /**
     * Generate a pseudorandom number using Math.random().
     *
     * @param lower minimum returned random number, inclusive
     * @param upper maximum returned random number, exclusive
     * @return the generated pseudorandom
     */
    public static long randomLong(long lower, long upper) {
        Preconditions.checkState(upper > lower);
        long interval = upper - lower;
        // Generate a random int on [lower, upper)
        double rand = Math.floor(Math.random() * interval) + lower;
        // Shouldn't happen
        if (rand >= upper)
            rand = upper - 1;
        // Cast and return
        return (long) rand;
    }

}
