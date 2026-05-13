# taosBenchmark

## taosBenchmark

#### 1.1 New command line arguments:

  -v, --vgroups=NUMBER       Specify Vgroups number for creating database, only
                                         valid with daemon version 3.0+

#### 1.2 Scope of TDengine versionchildtable_limit

TDengine 3.0+

## taosBenchmark support to write data in a segmental range child table

#### 2.1 add two new parameters  "childtable_from" and "childtable_to"

 to allow the user to specify a  range of child tables for data writing. taosBenchmark will generate a  series of table names with childtable_prefix and a number between  childtable_from and childtable_to. The valid number will be larger or  equal childtable_from and less than childtable_to, which means
the range is [childtable_from, childtable_to). 

#### 2.2 Scope of TDengine version

TDengine 2.x and TDengine 3.0+

#### 2.3 Incompatible impact:

If the JSON file contains the childtable_from and childtable_to, thhe childtable_limit and childtable_offset will be invalid.
 
#### 2.4 Add another parameter "continue_if_fail" to allow the user to define behavior if failed to write data.

"continue_if_fail":  "no", means taosBenchmark exit if failed. It's default behavior.
"continue_if_fail": "yes", means taosBenchmark will warn user but continue to execute if failed"
"continue_if_fail": "smart", means taosBenchmark will create childtable if the table is not exist.

#### 2.5 Limitation

Only work with SQL insertion.
