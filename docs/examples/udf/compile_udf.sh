# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set +e

rm -rf /tmp/udf/libbitand.so /tmp/udf/libl2norm.so /tmp/udf/libgpd.so /tmp/udf/libperm_entropy.so
mkdir -p /tmp/udf
echo "compile udf bit_and and sqr_sum"
gcc -fPIC -shared cases/12-UDFs/sh/bit_and.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libbitand.so
gcc -fPIC -shared cases/12-UDFs/sh/l2norm.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libl2norm.so
gcc -fPIC -shared cases/12-UDFs/sh/gpd.c -I../../include/libs/function/ -I../../include/client -I../../include/util   -o /tmp/udf/libgpd.so
# perm_entropy: aggregate UDF (accumulate-all-data-then-compute pattern)
gcc -fPIC -shared docs/examples/udf/perm_entropy.c -I../../include/libs/function/ -I../../include/client -I../../include/util -lm -o /tmp/udf/libperm_entropy.so
echo "debug show /tmp/udf/*.so"
ls /tmp/udf/*.so

