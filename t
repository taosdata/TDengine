feat(repair): add force-mode wal scheduling for taosd -r

  - add tRepairNeedRunWalForceRepair and tRepairBuildVnodeTargetPath
  - wire force+wal scheduling in dmMain via walInit and walOpen/walClose per vnode
  - persist wal step progress/state/log with existing repair session flow
  - add common tests for wal-force predicate and vnode target path builder
  - update task/progress/findings docs for T3.1 completion

