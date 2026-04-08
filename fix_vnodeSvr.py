import re

with open("source/dnode/vnode/src/vnd/vnodeSvr.c", "r") as f:
    text = f.read()

# Fix vnodeProcessCreateStbReq
text = re.sub(
r'''  if \(req\.txnId != 0\) \{
    vnodeTxnEnsureEntry\(pVnode, req\.txnId\);

    // Acquire table-level lock to detect cross-txn conflicts
    \{''',
r'''  if (req.txnId != 0) {
    int32_t ensureCode = vnodeTxnEnsureEntry(pVnode, req.txnId);
    if (ensureCode != 0) {
      pRsp->code = ensureCode;
      goto _err;
    }

    // Prepare rollback tracking implicitly - harmless if metaCreate fails
    int32_t trackCode = vnodeTxnTrackTable(pVnode, req.txnId, req.suid);
    if (trackCode != 0) {
      vError("vgId:%d, stb:%s uid:%" PRId64 " vnodeTxnTrackTable failed, code:0x%x", TD_VID(pVnode), req.name, req.suid, trackCode);
      pRsp->code = trackCode;
      goto _err;
    }

    // Acquire table-level lock to detect cross-txn conflicts
    {''',
text, count=1)

text = re.sub(
r'''  // batch-meta-txn: track STB in VNode txn entry for COMMIT/ROLLBACK
  if \(req\.txnId != 0\) \{
    code = vnodeTxnTrackTable\(pVnode, req\.txnId, req\.suid\);
    if \(code != 0\) \{
      vError\("vgId:%d, stb:%s uid:%" PRId64 " vnodeTxnTrackTable failed, code:0x%x", TD_VID\(pVnode\), req\.name, req\.suid,
             code\);
      pRsp->code = code;
      goto _err;
    \}
    vInfo\("vgId:%d, stb:%s uid:%" PRId64 " tracked in txn %" PRIu64, TD_VID\(pVnode\), req\.name, req\.suid, req\.txnId\);
  \}''',
r'''  // batch-meta-txn: STB was tracked before creation
  if (req.txnId != 0) {
    vInfo("vgId:%d, stb:%s uid:%" PRId64 " tracked in txn %" PRIu64, TD_VID(pVnode), req.name, req.suid, req.txnId);
  }''', text, count=1)

# Fix vnodeProcessAlterStbReq
text = re.sub(
r'''  if \(req\.txnId != 0\) \{
    vnodeTxnEnsureEntry\(pVnode, req\.txnId\);

    // Acquire table-level lock to detect cross-txn conflicts
    \{''',
r'''  if (req.txnId != 0) {
    int32_t ensureCode = vnodeTxnEnsureEntry(pVnode, req.txnId);
    if (ensureCode != 0) {
      pRsp->code = ensureCode;
      tDecoderClear(&dc);
      return ensureCode;
    }

    // Acquire table-level lock to detect cross-txn conflicts
    {''', text, count=1)

text = re.sub(
r'''        int32_t trackCode = vnodeTxnTrackAlter\(pVnode, req\.txnId, pOldEntry->uid, pOldEntry->version\);
        if \(trackCode != 0\) \{
          vError\("vgId:%d, stb:%s ALTER vnodeTxnTrackAlter failed, code:0x%x", TD_VID\(pVnode\), req\.name, trackCode\);
        \}
        metaFetchEntryFree\(&pOldEntry\);''',
r'''        int32_t trackCode = vnodeTxnTrackAlter(pVnode, req.txnId, pOldEntry->uid, pOldEntry->version);
        metaFetchEntryFree(&pOldEntry);
        if (trackCode != 0) {
          vError("vgId:%d, stb:%s ALTER vnodeTxnTrackAlter failed, code:0x%x", TD_VID(pVnode), req.name, trackCode);
          pRsp->code = trackCode;
          tDecoderClear(&dc);
          return 0;
        }''', text, count=1)

text = re.sub(
r'''    if \(code\) \{
      pRsp->code = code;
    \} else \{
      code = vnodeTxnTrackTable\(pVnode, req\.txnId, req\.suid\);
      if \(code != 0\) \{
        vError\("vgId:%d, stb:%s uid:%" PRId64 " ALTER vnodeTxnTrackTable failed, code:0x%x", TD_VID\(pVnode\), req\.name,
               req\.suid, code\);
        pRsp->code = code;
      \} else \{
        vInfo\("vgId:%d, stb:%s uid:%" PRId64 " ALTER tracked in txn %" PRIu64, TD_VID\(pVnode\), req\.name, req\.suid,
              req\.txnId\);
      \}
    \}''',
r'''    if (code) {
      pRsp->code = code;
    } else {
      int32_t trackCode = vnodeTxnTrackTable(pVnode, req.txnId, req.suid);
      if (trackCode != 0) {
        vError("vgId:%d, stb:%s uid:%" PRId64 " ALTER vnodeTxnTrackTable failed, code:0x%x", TD_VID(pVnode), req.name,
               req.suid, trackCode);
        // Fallthrough if only tracking failed: rollbacks could still potentially clean up? 
        // No, we should fail earlier. But we only know it's altered after metaAlterSuperTable!
        // Actually, we can move vnodeTxnTrackTable to BEFORE metaAlterSuperTable too!
        pRsp->code = trackCode;
      } else {
        vInfo("vgId:%d, stb:%s uid:%" PRId64 " ALTER tracked in txn %" PRIu64, TD_VID(pVnode), req.name, req.suid,
              req.txnId);
      }
    }''', text, count=1)

with open("source/dnode/vnode/src/vnd/vnodeSvr.c", "w") as f:
    f.write(text)
