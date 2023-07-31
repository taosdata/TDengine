#!/usr/bin/expect 
set packageName [lindex $argv 0]
set packageSuffix [lindex $argv 1]
set timeout 30 
if { ${packageSuffix} == "deb" } {
    spawn  dpkg -i ${packageName}
} elseif { ${packageSuffix} == "rpm"} {
    spawn rpm -ivh ${packageName}
}
expect "*one:"
send  "\r"
expect "*skip:"
send  "\r" 

expect eof
