# ~/.bashrc: executed by bash(1) for non-login shells.

# set the alias and path 

export PATH=$PATH:~/dev/build/bin

rootDir1="~/test"
rootDir2="~/second"
rootDir3="~/third"

alias taos1="taos -c ${rootDir1}/cfg"
alias taosd1="taosd -c ${rootDir1}/cfg"
alias taosm1="taosm -c ${rootDir1}/cfg"
alias tsim1="tsim -c ${rootDir1}/cfg"

alias taos2="taos -c ${rootDir2}/cfg"
alias taosd2="taosd -c ${rootDir2}/cfg"
alias taosm2="taosm -c ${rootDir2}/cfg"
alias tsim2="tsim -c ${rootDir2}/cfg"

alias taos3="taos -c ${rootDir3}/cfg"
alias taosd3="taosd -c ${rootDir3}/cfg"
alias taosm3="taosm -c ${rootDir3}/cfg"
alias tsim3="tsim -c ${rootDir3}/cfg"

alias tsclean="rm -rf ${rootDir1}/data/*; rm -rf ${rootDir1}/log/*; rm -rf ${rootDir2}/data/*; rm -rf ${rootDir2}/log/*; rm -rf ${rootDir3}/data/*; rm -rf ${rootDir3}/log/*;"

ulimit -c 10240000

