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

namespace Apache\Avro\Tests;

use Apache\Avro\AvroDebug;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOBinaryEncoder;
use PHPUnit\Framework\TestCase;

class FloatIntEncodingTest extends TestCase
{
  const FLOAT_TYPE = 'float';
  const DOUBLE_TYPE = 'double';

  static $FLOAT_NAN;
  static $FLOAT_POS_INF;
  static $FLOAT_NEG_INF;
  static $DOUBLE_NAN;
  static $DOUBLE_POS_INF;
  static $DOUBLE_NEG_INF;

  static $LONG_BITS_NAN;
  static $LONG_BITS_POS_INF;
  static $LONG_BITS_NEG_INF;
  static $INT_BITS_NAN;
  static $INT_BITS_POS_INF;
  static $INT_BITS_NEG_INF;

  static function make_special_vals()
  {
    self::$DOUBLE_NAN     = (double) NAN;
    self::$DOUBLE_POS_INF = (double) INF;
    self::$DOUBLE_NEG_INF = (double) -INF;
    self::$FLOAT_NAN      = (float) NAN;
    self::$FLOAT_POS_INF  = (float) INF;
    self::$FLOAT_NEG_INF  = (float) -INF;

    self::$LONG_BITS_NAN     = strrev(pack('H*', '7ff8000000000000'));
    self::$LONG_BITS_POS_INF = strrev(pack('H*', '7ff0000000000000'));
    self::$LONG_BITS_NEG_INF = strrev(pack('H*', 'fff0000000000000'));
    self::$INT_BITS_NAN      = strrev(pack('H*', '7fc00000'));
    self::$INT_BITS_POS_INF  = strrev(pack('H*', '7f800000'));
    self::$INT_BITS_NEG_INF  = strrev(pack('H*', 'ff800000'));
  }

  function setUp(): void
  {
    self::make_special_vals();
  }

  function test_special_values()
  {
    $this->assertIsFloat(self::$FLOAT_NAN, 'float NaN is a float');
    $this->assertTrue(is_nan(self::$FLOAT_NAN), 'float NaN is NaN');
    $this->assertFalse(is_infinite(self::$FLOAT_NAN), 'float NaN is not infinite');

    $this->assertIsFloat(self::$FLOAT_POS_INF, 'float pos infinity is a float');
    $this->assertTrue(is_infinite(self::$FLOAT_POS_INF), 'float pos infinity is infinite');
    $this->assertTrue(0 < self::$FLOAT_POS_INF, 'float pos infinity is greater than 0');
    $this->assertFalse(is_nan(self::$FLOAT_POS_INF), 'float pos infinity is not NaN');

    $this->assertIsFloat(self::$FLOAT_NEG_INF, 'float neg infinity is a float');
    $this->assertTrue(is_infinite(self::$FLOAT_NEG_INF), 'float neg infinity is infinite');
    $this->assertTrue(0 > self::$FLOAT_NEG_INF, 'float neg infinity is less than 0');
    $this->assertFalse(is_nan(self::$FLOAT_NEG_INF), 'float neg infinity is not NaN');

    $this->assertIsFloat(self::$DOUBLE_NAN, 'double NaN is a double');
    $this->assertTrue(is_nan(self::$DOUBLE_NAN), 'double NaN is NaN');
    $this->assertFalse(is_infinite(self::$DOUBLE_NAN), 'double NaN is not infinite');

    $this->assertIsFloat(self::$DOUBLE_POS_INF, 'double pos infinity is a double');
    $this->assertTrue(is_infinite(self::$DOUBLE_POS_INF), 'double pos infinity is infinite');
    $this->assertTrue(0 < self::$DOUBLE_POS_INF, 'double pos infinity is greater than 0');
    $this->assertFalse(is_nan(self::$DOUBLE_POS_INF), 'double pos infinity is not NaN');

    $this->assertIsFloat(self::$DOUBLE_NEG_INF, 'double neg infinity is a double');
    $this->assertTrue(is_infinite(self::$DOUBLE_NEG_INF), 'double neg infinity is infinite');
    $this->assertTrue(0 > self::$DOUBLE_NEG_INF, 'double neg infinity is less than 0');
    $this->assertFalse(is_nan(self::$DOUBLE_NEG_INF), 'double neg infinity is not NaN');

  }

  function special_vals_provider()
  {
    self::make_special_vals();
    return array(array(self::DOUBLE_TYPE, self::$DOUBLE_POS_INF, self::$LONG_BITS_POS_INF),
                 array(self::DOUBLE_TYPE, self::$DOUBLE_NEG_INF, self::$LONG_BITS_NEG_INF),
                 array(self::FLOAT_TYPE, self::$FLOAT_POS_INF, self::$INT_BITS_POS_INF),
                 array(self::FLOAT_TYPE, self::$FLOAT_NEG_INF, self::$INT_BITS_NEG_INF));
  }

  /**
   * @dataProvider special_vals_provider
   */
  function test_encoding_special_values($type, $val, $bits)
  {
    $this->assert_encode_values($type, $val, $bits);
  }

  function nan_vals_provider()
  {
    self::make_special_vals();
    return array(array(self::DOUBLE_TYPE, self::$DOUBLE_NAN, self::$LONG_BITS_NAN),
                 array(self::FLOAT_TYPE, self::$FLOAT_NAN, self::$INT_BITS_NAN));
  }

  /**
   * @dataProvider nan_vals_provider
   */
  function test_encoding_nan_values($type, $val, $bits)
  {
    $this->assert_encode_nan_values($type, $val, $bits);
  }

  function normal_vals_provider()
  {
    $ruby_to_generate_vals =<<<_RUBY
      def d2lb(d); [d].pack('E') end
      dary = (-10..10).to_a + [-1234.2132, -211e23]
      dary.each {|x| b = d2lb(x); puts %/array(self::DOUBLE_TYPE, (double) #{x}, #{b.inspect}, '#{b.unpack('h*')[0]}'),/}
      def f2ib(f); [f].pack('e') end
      fary = (-10..10).to_a + [-1234.5, -211.3e6]
      fary.each {|x| b = f2ib(x); puts %/array(self::FLOAT_TYPE, (float) #{x}, #{b.inspect}, '#{b.unpack('h*')[0]}'),/}
_RUBY;

    return array(
                 array(self::DOUBLE_TYPE, (double) -10, "\000\000\000\000\000\000$\300", '000000000000420c'),
                 array(self::DOUBLE_TYPE, (double) -9, "\000\000\000\000\000\000\"\300", '000000000000220c'),
                 array(self::DOUBLE_TYPE, (double) -8, "\000\000\000\000\000\000 \300", '000000000000020c'),
                 array(self::DOUBLE_TYPE, (double) -7, "\000\000\000\000\000\000\034\300", '000000000000c10c'),
                 array(self::DOUBLE_TYPE, (double) -6, "\000\000\000\000\000\000\030\300", '000000000000810c'),
                 array(self::DOUBLE_TYPE, (double) -5, "\000\000\000\000\000\000\024\300", '000000000000410c'),
                 array(self::DOUBLE_TYPE, (double) -4, "\000\000\000\000\000\000\020\300", '000000000000010c'),
            /**/ array(self::DOUBLE_TYPE, (double) -3, "\000\000\000\000\000\000\010\300", '000000000000800c'),
                 array(self::DOUBLE_TYPE, (double) -2, "\000\000\000\000\000\000\000\300", '000000000000000c'),
                 array(self::DOUBLE_TYPE, (double) -1, "\000\000\000\000\000\000\360\277", '0000000000000ffb'),
                 array(self::DOUBLE_TYPE, (double) 0, "\000\000\000\000\000\000\000\000", '0000000000000000'),
                 array(self::DOUBLE_TYPE, (double) 1, "\000\000\000\000\000\000\360?", '0000000000000ff3'),
                 array(self::DOUBLE_TYPE, (double) 2, "\000\000\000\000\000\000\000@", '0000000000000004'),
            /**/ array(self::DOUBLE_TYPE, (double) 3, "\000\000\000\000\000\000\010@", '0000000000008004'),
                 array(self::DOUBLE_TYPE, (double) 4, "\000\000\000\000\000\000\020@", '0000000000000104'),
                 array(self::DOUBLE_TYPE, (double) 5, "\000\000\000\000\000\000\024@", '0000000000004104'),
                 array(self::DOUBLE_TYPE, (double) 6, "\000\000\000\000\000\000\030@", '0000000000008104'),
                 array(self::DOUBLE_TYPE, (double) 7, "\000\000\000\000\000\000\034@", '000000000000c104'),
                 array(self::DOUBLE_TYPE, (double) 8, "\000\000\000\000\000\000 @", '0000000000000204'),
                 array(self::DOUBLE_TYPE, (double) 9, "\000\000\000\000\000\000\"@", '0000000000002204'),
                 array(self::DOUBLE_TYPE, (double) 10, "\000\000\000\000\000\000$@", '0000000000004204'),
            /**/ array(self::DOUBLE_TYPE, (double) -1234.2132, "\007\316\031Q\332H\223\300", '70ec9115ad84390c'),
                 array(self::DOUBLE_TYPE, (double) -2.11e+25, "\311\260\276J\031t1\305", '9c0beba49147135c'),

                 array(self::FLOAT_TYPE, (float) -10, "\000\000 \301", '0000021c'),
                 array(self::FLOAT_TYPE, (float) -9, "\000\000\020\301", '0000011c'),
                 array(self::FLOAT_TYPE, (float) -8, "\000\000\000\301", '0000001c'),
                 array(self::FLOAT_TYPE, (float) -7, "\000\000\340\300", '00000e0c'),
                 array(self::FLOAT_TYPE, (float) -6, "\000\000\300\300", '00000c0c'),
                 array(self::FLOAT_TYPE, (float) -5, "\000\000\240\300", '00000a0c'),
                 array(self::FLOAT_TYPE, (float) -4, "\000\000\200\300", '0000080c'),
                 array(self::FLOAT_TYPE, (float) -3, "\000\000@\300", '0000040c'),
                 array(self::FLOAT_TYPE, (float) -2, "\000\000\000\300", '0000000c'),
                 array(self::FLOAT_TYPE, (float) -1, "\000\000\200\277", '000008fb'),
                 array(self::FLOAT_TYPE, (float) 0, "\000\000\000\000", '00000000'),
                 array(self::FLOAT_TYPE, (float) 1, "\000\000\200?", '000008f3'),
                 array(self::FLOAT_TYPE, (float) 2, "\000\000\000@", '00000004'),
                 array(self::FLOAT_TYPE, (float) 3, "\000\000@@", '00000404'),
                 array(self::FLOAT_TYPE, (float) 4, "\000\000\200@", '00000804'),
                 array(self::FLOAT_TYPE, (float) 5, "\000\000\240@", '00000a04'),
                 array(self::FLOAT_TYPE, (float) 6, "\000\000\300@", '00000c04'),
                 array(self::FLOAT_TYPE, (float) 7, "\000\000\340@", '00000e04'),
                 array(self::FLOAT_TYPE, (float) 8, "\000\000\000A", '00000014'),
                 array(self::FLOAT_TYPE, (float) 9, "\000\000\020A", '00000114'),
                 array(self::FLOAT_TYPE, (float) 10, "\000\000 A", '00000214'),
                 array(self::FLOAT_TYPE, (float) -1234.5, "\000P\232\304", '0005a94c'),
                 array(self::FLOAT_TYPE, (float) -211300000.0, "\352\202I\315", 'ae2894dc'),
      );
  }

  function float_vals_provider()
  {
    $ary = array();

    foreach ($this->normal_vals_provider() as $values)
      if (self::FLOAT_TYPE == $values[0])
        $ary []= array($values[0], $values[1], $values[2]);

    return $ary;
  }

  function double_vals_provider()
  {
    $ary = array();

    foreach ($this->normal_vals_provider() as $values)
      if (self::DOUBLE_TYPE == $values[0])
        $ary []= array($values[0], $values[1], $values[2]);

    return $ary;
  }


  /**
   * @dataProvider float_vals_provider
   */
  function test_encoding_float_values($type, $val, $bits)
  {
    $this->assert_encode_values($type, $val, $bits);
  }

  /**
   * @dataProvider double_vals_provider
   */
  function test_encoding_double_values($type, $val, $bits)
  {
    $this->assert_encode_values($type, $val, $bits);
  }

  function assert_encode_values($type, $val, $bits)
  {
    if (self::FLOAT_TYPE == $type)
    {
      $decoder = array(AvroIOBinaryDecoder::class, 'intBitsToFloat');
      $encoder = array(AvroIOBinaryEncoder::class, 'floatToIntBits');
    }
    else
    {
      $decoder = array(AvroIOBinaryDecoder::class, 'longBitsToDouble');
      $encoder = array(AvroIOBinaryEncoder::class, 'doubleToLongBits');
    }

    $decoded_bits_val = call_user_func($decoder, $bits);
    $this->assertEquals($val, $decoded_bits_val,
                        sprintf("%s\n expected: '%f'\n    given: '%f'",
                                'DECODED BITS', $val, $decoded_bits_val));

    $encoded_val_bits = call_user_func($encoder, $val);
    $this->assertEquals($bits, $encoded_val_bits,
                        sprintf("%s\n expected: '%s'\n    given: '%s'",
                                'ENCODED VAL',
                                AvroDebug::hexString($bits),
                                AvroDebug::hexString($encoded_val_bits)));

    $round_trip_value = call_user_func($decoder, $encoded_val_bits);
    $this->assertEquals($val, $round_trip_value,
                        sprintf("%s\n expected: '%f'\n     given: '%f'",
                                'ROUND TRIP BITS', $val, $round_trip_value));
  }

  function assert_encode_nan_values($type, $val, $bits)
  {
    if (self::FLOAT_TYPE == $type)
    {
      $decoder = array(AvroIOBinaryDecoder::class, 'intBitsToFloat');
      $encoder = array(AvroIOBinaryEncoder::class, 'floatToIntBits');
    }
    else
    {
      $decoder = array(AvroIOBinaryDecoder::class, 'longBitsToDouble');
      $encoder = array(AvroIOBinaryEncoder::class, 'doubleToLongBits');
    }

    $decoded_bits_val = call_user_func($decoder, $bits);
    $this->assertTrue(is_nan($decoded_bits_val),
                      sprintf("%s\n expected: '%f'\n    given: '%f'",
                              'DECODED BITS', $val, $decoded_bits_val));

    $encoded_val_bits = call_user_func($encoder, $val);
    $this->assertEquals($bits, $encoded_val_bits,
                        sprintf("%s\n expected: '%s'\n    given: '%s'",
                                'ENCODED VAL',
                                AvroDebug::hexString($bits),
                                AvroDebug::hexString($encoded_val_bits)));

    $round_trip_value = call_user_func($decoder, $encoded_val_bits);
    $this->assertTrue(is_nan($round_trip_value),
                      sprintf("%s\n expected: '%f'\n     given: '%f'",
                              'ROUND TRIP BITS', $val, $round_trip_value));
  }
}
