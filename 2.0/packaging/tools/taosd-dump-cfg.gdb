# Usage:
# sudo gdb -x ./taosd-dump-cfg.gdb
 
define attach_pidof
    if $argc != 1
        help attach_pidof
    else
        shell echo -e "\
set \$PID = "$(echo $(pidof $arg0) 0 | cut -d " " -f 1)"\n\
if \$PID > 0\n\
    attach "$(pidof -s $arg0)"\n\
else\n\
    print \"Process '"$arg0"' not found\"\n\
end" > /tmp/gdb.pidof
    source /tmp/gdb.pidof
    end
end
 
document attach_pidof
Attach to process by name
Usage: attach_pidof PROG_NAME
end
 
set $TAOS_CFG_VTYPE_INT8      = 0
set $TAOS_CFG_VTYPE_INT16     = 1
set $TAOS_CFG_VTYPE_INT32     = 2
set $TAOS_CFG_VTYPE_FLOAT     = 3
set $TAOS_CFG_VTYPE_STRING    = 4
set $TAOS_CFG_VTYPE_IPSTR     = 5
set $TAOS_CFG_VTYPE_DIRECTORY = 6
 
set $TSDB_CFG_CTYPE_B_CONFIG    = 1U
set $TSDB_CFG_CTYPE_B_SHOW      = 2U
set $TSDB_CFG_CTYPE_B_LOG       = 4U
set $TSDB_CFG_CTYPE_B_CLIENT    = 8U
set $TSDB_CFG_CTYPE_B_OPTION    = 16U
set $TSDB_CFG_CTYPE_B_NOT_PRINT = 32U
 
set $TSDB_CFG_PRINT_LEN = 53
 
define print_blank
  if $argc == 1
    set $blank_len = $arg0
    while $blank_len > 0
      printf "%s", " "
      set $blank_len = $blank_len - 1
    end
  end
end
 
define dump_cfg
  if $argc != 1
    help dump_cfg
  else
    set $blen = $TSDB_CFG_PRINT_LEN - (int)strlen($arg0.option)
    if $blen < 0
      $blen = 0
    end
    #printf "%s: %d\n", "******blen: ", $blen
    printf "%s: ", $arg0.option
    print_blank $blen
     
    if $arg0.valType == $TAOS_CFG_VTYPE_INT8
      printf "%d\n", *((int8_t *) $arg0.ptr)
    else
      if $arg0.valType == $TAOS_CFG_VTYPE_INT16
        printf "%d\n", *((int16_t *) $arg0.ptr)
      else
    if $arg0.valType == $TAOS_CFG_VTYPE_INT32
        printf "%d\n", *((int32_t *) $arg0.ptr)
    else
      if $arg0.valType == $TAOS_CFG_VTYPE_FLOAT
        printf "%f\n", *((float *) $arg0.ptr)
      else
        printf "%s\n", $arg0.ptr
      end
    end
      end
    end
  end
end
 
document dump_cfg
Dump a cfg entry
Usage: dump_cfg cfg
end
 
set pagination off
 
attach_pidof taosd
 
set $idx=0
#print tsGlobalConfigNum
#set $end=$1
set $end=tsGlobalConfigNum
 
p "*=*=*=*=*=*=*=*=*= taos global config:"
#while ($idx .lt. $end)
while ($idx < $end)
  # print tsGlobalConfig[$idx].option
  set $cfg = tsGlobalConfig[$idx]
  set $tsce = tscEmbedded
#  p "1"
  if ($tsce == 0)
    if !($cfg.cfgType & $TSDB_CFG_CTYPE_B_CLIENT)
    end
  else
    if $cfg.cfgType & $TSDB_CFG_CTYPE_B_NOT_PRINT
    else
      if !($cfg.cfgType & $TSDB_CFG_CTYPE_B_SHOW)
      else
    dump_cfg $cfg
      end
    end
  end
   
  set $idx=$idx+1
end
 
set $idx=0
 
p "*=*=*=*=*=*=*=*=*= taos local config:"
while ($idx < $end)
  set $cfg = tsGlobalConfig[$idx]
  set $tsce = tscEmbedded
  if ($tsce == 0)
    if !($cfg.cfgType & $TSDB_CFG_CTYPE_B_CLIENT)
    end
  else
    if $cfg.cfgType & $TSDB_CFG_CTYPE_B_NOT_PRINT
    else
      if ($cfg.cfgType & $TSDB_CFG_CTYPE_B_SHOW)
      else
    dump_cfg $cfg
      end
    end
  end
   
  set $idx=$idx+1
end
 
detach
 
quit
