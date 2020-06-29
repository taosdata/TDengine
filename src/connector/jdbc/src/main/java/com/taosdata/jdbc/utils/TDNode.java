package com.taosdata.jdbc.utils;

public class TDNode {
    
    private int index;
    private int running;
    private int deployed;
    private boolean testCluster;
    private int valgrind;
    private String path;

    public TDNode(int index) {
        this.index = index;
        running = 0;
        deployed = 0;
        testCluster = false;
        valgrind = 0;        
    }

    public void setTestCluster(boolean testCluster) {
        this.testCluster = testCluster;
    }

    public void setValgrind(boolean valgrind) {
        this.valgrind = valgrind;
    }

    public int getDataSize() {
        totalSize = 0;

        if(deployed == 1) {

        }
    }

    def getDataSize(self):
        totalSize = 0

        if (self.deployed == 1):
            for dirpath, dirnames, filenames in .walk(self.dataDir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)

                    if not os.path.islink(fp):
                        totalSize = totalSize + os.path.getsize(fp)

        return totalSize



}