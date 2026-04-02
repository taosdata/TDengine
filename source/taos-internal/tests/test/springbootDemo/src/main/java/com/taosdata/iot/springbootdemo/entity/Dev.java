package com.taosdata.iot.springbootdemo.entity;

//import javax.persistence.Column;
//import javax.persistence.Entity;
//import javax.persistence.Id;
//import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * @author Jiangyi Hou
 * @since 18-12-12
 */

//@Entity
//@Table(name = "dev")
public class Dev {
//    @Id
    Timestamp ts;
//    @Column
    Integer c1;

    public Timestamp getTs() {
        return ts;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Integer getC1() {
        return c1;
    }

    public void setC1(Integer c1) {
        this.c1 = c1;
    }
}
