package com.taos.example.dao;

import java.sql.Timestamp;

public class Meter {

  private Timestamp ts;
  private float current;
  private int voltage;
  private float phase;
  private int groupId;
  private String location;

  public Timestamp getTs() {
    return ts;
  }

  public void setTs(Timestamp ts) {
    this.ts = ts;
  }

  public float getCurrent() {
    return current;
  }

  public void setCurrent(float current) {
    this.current = current;
  }

  public int getVoltage() {
    return voltage;
  }

  public void setVoltage(int voltage) {
    this.voltage = voltage;
  }

  public float getPhase() {
    return phase;
  }

  public void setPhase(float phase) {
    this.phase = phase;
  }

  public int getGroupId() {
    return groupId;
  }

  public void setGroupId(int groupId) {
    this.groupId = groupId;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }
}
