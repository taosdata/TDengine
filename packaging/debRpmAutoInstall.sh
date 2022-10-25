#!/usr/bin/expect 
set packgeName [lindex $argv 0]
set packageSuffix [lindex $argv 1]
set timeout 3 
if { ${packageSuffix} == "deb" } {
    spawn  dpkg -i ${packgeName}
} elseif { ${packageSuffix} == "rpm"} {
    spawn rpm -ivh ${packgeName}
}
expect "*one:"
send  "\r"
expect "*skip:"
send  "\r" 

expect eof