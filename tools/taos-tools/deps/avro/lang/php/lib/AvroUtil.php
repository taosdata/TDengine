<?php

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro;

/**
 * Class for static utility methods used in Avro.
 *
 * @package Avro
 */
class AvroUtil
{
    /**
     * Determines whether the given array is an associative array
     * (what is termed a map, hash, or dictionary in other languages)
     * or a list (an array with monotonically increasing integer indicies
     * starting with zero).
     *
     * @param array $ary array to test
     * @returns true if the array is a list and false otherwise.
     *
     */
    public static function isList($ary): bool
    {
        if (is_array($ary)) {
            $i = 0;
            foreach ($ary as $k => $v) {
                if ($i !== $k) {
                    return false;
                }
                $i++;
            }
            return true;
        }
        return false;
    }
}
