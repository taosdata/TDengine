package com.taosdata.tsync.entity.config;

public class DatabaseConfiguration extends Configuration {
    private String name;
    private Integer replica;
    private Integer quorum;
    private Integer days;
    private Integer[] keep;
    private Integer cache;
    private Integer blocks;
    private Integer minrows;
    private Integer maxrows;
    private Integer wallevel;
    private Integer fsync;
    private Integer comp;
    private String precision;
    private Integer update;

    public DatabaseConfiguration() {
        super(ConfigurationType.DATABASE);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getReplica() {
        return replica;
    }

    public void setReplica(Integer replica) {
        this.replica = replica;
    }

    public Integer getQuorum() {
        return quorum;
    }

    public void setQuorum(Integer quorum) {
        this.quorum = quorum;
    }

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }

    public Integer[] getKeep() {
        return keep;
    }

    public void setKeep(Integer[] keep) {
        this.keep = keep;
    }

    public Integer getCache() {
        return cache;
    }

    public void setCache(Integer cache) {
        this.cache = cache;
    }

    public Integer getBlocks() {
        return blocks;
    }

    public void setBlocks(Integer blocks) {
        this.blocks = blocks;
    }

    public Integer getMinrows() {
        return minrows;
    }

    public void setMinrows(Integer minrows) {
        this.minrows = minrows;
    }

    public Integer getMaxrows() {
        return maxrows;
    }

    public void setMaxrows(Integer maxrows) {
        this.maxrows = maxrows;
    }

    public Integer getWallevel() {
        return wallevel;
    }

    public void setWallevel(Integer wallevel) {
        this.wallevel = wallevel;
    }

    public Integer getFsync() {
        return fsync;
    }

    public void setFsync(Integer fsync) {
        this.fsync = fsync;
    }

    public Integer getComp() {
        return comp;
    }

    public void setComp(Integer comp) {
        this.comp = comp;
    }

    public String getPrecision() {
        return precision;
    }

    public void setPrecision(String precision) {
        this.precision = precision;
    }

    public Integer getUpdate() {
        return update;
    }

    public void setUpdate(Integer update) {
        this.update = update;
    }
}
