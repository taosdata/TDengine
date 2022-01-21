#!/bin/bash
[[ -x /usr/bin/taosadapter ]] && /usr/bin/taosadapter &
taosd
