#!/bin/bash

script_dir=$(dirname $(readlink -m "$0"))
${script_dir}/install.sh client
