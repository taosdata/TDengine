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
 * Methods for handling 64-bit operations using the GMP extension.
 *
 * This is a naive and hackish implementation that is intended
 * to work well enough to support Avro. It has not been tested
 * beyond what's needed to decode and encode long values.
 *
 * @package Avro
 */
class AvroGMP
{
    /**
     * @var resource memoized GMP resource for zero
     */
    private static $gmp_0;
    /**
     * @var resource memoized GMP resource for one (1)
     */
    private static $gmp_1;
    /**
     * @var resource memoized GMP resource for two (2)
     */
    private static $gmp_2;
    /**
     * @var resource memoized GMP resource for 0x7f
     */
    private static $gmp_0x7f;
    /**
     * @var resource memoized GMP resource for 64-bit ~0x7f
     */
    private static $gmp_n0x7f;
    /**
     * @var resource memoized GMP resource for 64-bits of 1
     */
    private static $gmp_0xfs;

    /**
     * @param int|str $n integer (or string representation of integer) to encode
     * @return string $bytes of the long $n encoded per the Avro spec
     */
    public static function encodeLong($n)
    {
        $g = gmp_init($n);
        $g = gmp_xor(
            self::shiftLeft($g, 1),
            self::shiftRight($g, 63)
        );
        $bytes = '';
        while (0 != gmp_cmp(self::gmp_0(), gmp_and($g, self::gmp_n0x7f()))) {
            $bytes .= chr(gmp_intval(gmp_and($g, self::gmp_0x7f())) | 0x80);
            $g = self::shiftRight($g, 7);
        }
        $bytes .= chr(gmp_intval($g));
        return $bytes;
    }

    /**
     * @interal Only works up to shift 63 (doesn't wrap bits around).
     * @param resource|int|string $g
     * @param int $shift number of bits to shift left
     * @returns resource $g shifted left
     */
    public static function shiftLeft($g, $shift)
    {
        if (0 == $shift) {
            return $g;
        }

        if (0 > gmp_sign($g)) {
            $g = self::gmpTwosComplement($g);
        }

        $m = gmp_mul($g, gmp_pow(self::gmp_2(), $shift));
        $m = gmp_and($m, self::gmp_0xfs());
        if (gmp_testbit($m, 63)) {
            $m = gmp_neg(gmp_add(
                gmp_and(gmp_com($m), self::gmp_0xfs()),
                self::gmp_1()
            ));
        }
        return $m;
    }

    /**
     * @param GMP resource
     * @returns GMP resource 64-bit two's complement of input.
     */
    public static function gmpTwosComplement($g)
    {
        return gmp_neg(gmp_sub(gmp_pow(self::gmp_2(), 64), $g));
    }

    /**
     * Arithmetic right shift
     * @param resource|int|string $g
     * @param int $shift number of bits to shift right
     * @returns resource $g shifted right $shift bits
     */
    public static function shiftRight($g, $shift)
    {
        if (0 == $shift) {
            return $g;
        }

        if (0 <= gmp_sign($g)) {
            $m = gmp_div($g, gmp_pow(self::gmp_2(), $shift));
        } else // negative
        {
            $g = gmp_and($g, self::gmp_0xfs());
            $m = gmp_div($g, gmp_pow(self::gmp_2(), $shift));
            $m = gmp_and($m, self::gmp_0xfs());
            for ($i = 63; $i >= (63 - $shift); $i--) {
                gmp_setbit($m, $i);
            }

            $m = gmp_neg(gmp_add(
                gmp_and(gmp_com($m), self::gmp_0xfs()),
                self::gmp_1()
            ));
        }

        return $m;
    }

    // phpcs:disable PSR1.Methods.CamelCapsMethodName

    /**
     * @returns resource GMP resource for two (2)
     */
    private static function gmp_2()
    {
        if (!isset(self::$gmp_2)) {
            self::$gmp_2 = gmp_init('2');
        }
        return self::$gmp_2;
    }

    /**
     * @returns resource GMP resource for 64-bits of 1
     */
    private static function gmp_0xfs()
    {
        if (!isset(self::$gmp_0xfs)) {
            self::$gmp_0xfs = gmp_init('0xffffffffffffffff');
        }
        return self::$gmp_0xfs;
    }

    /**
     * @returns resource GMP resource for one (1)
     */
    private static function gmp_1()
    {
        if (!isset(self::$gmp_1)) {
            self::$gmp_1 = gmp_init('1');
        }
        return self::$gmp_1;
    }

    /**
     * @returns resource GMP resource for zero
     */
    private static function gmp_0()
    {
        if (!isset(self::$gmp_0)) {
            self::$gmp_0 = gmp_init('0');
        }
        return self::$gmp_0;
    }

    /**
     * @returns resource GMP resource for 64-bit ~0x7f
     */
    private static function gmp_n0x7f()
    {
        if (!isset(self::$gmp_n0x7f)) {
            self::$gmp_n0x7f = gmp_init('0xffffffffffffff80');
        }
        return self::$gmp_n0x7f;
    }

    /**
     * @returns resource GMP resource for 0x7f
     */
    private static function gmp_0x7f()
    {
        if (!isset(self::$gmp_0x7f)) {
            self::$gmp_0x7f = gmp_init('0x7f');
        }
        return self::$gmp_0x7f;
    }

    // phpcs:enable

    /**
     * @param int[] $bytes array of ascii codes of bytes to decode
     * @return string represenation of decoded long.
     */
    public static function decodeLongFromArray($bytes)
    {
        $b = array_shift($bytes);
        $g = gmp_init($b & 0x7f);
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $g = gmp_or($g, self::shiftLeft(($b & 0x7f), $shift));
            $shift += 7;
        }
        $val = gmp_xor(self::shiftRight($g, 1), gmp_neg(gmp_and($g, 1)));
        return gmp_strval($val);
    }
}
