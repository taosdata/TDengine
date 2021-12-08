#!/bin/bash
[[ -x /usr/bin/taosadataper ]] && /usr/bin/taosadapter &
taosd
