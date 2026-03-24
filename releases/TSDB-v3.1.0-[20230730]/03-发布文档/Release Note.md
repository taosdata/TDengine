# Release Note

**Note: Once you upgrade to this release, you can't go back to old version**
**Note: If there is stream, please drop them before upgrading**

New Features & Improvements
- Performance improvement for Join
- Performance improvement for order by non-primary key + limit
- Performance and memory usage improvement for stable order by primary key + limit
- Performance improvement for counting out of order data
- Improved fault tolerance when disk is broken
- New data type: Geometry
- Stream with fill history can be paused/resumed
- Load balance among level 0 disks (Enterprise only)
- Performance improvement in high cardinality case (Enterprise only)
- Compacting data doesn't block writing (Enterprise only)
- Data retention in multi-level storage doesn't block writing (Enterprise only)

Fixed Bugs
- Fixed some bugs found in previous releases
