package com.taosdata.jdbc.ws.tmq.meta;

import java.util.List;
import java.util.Objects;

public class MetaCreateChildTable extends Meta {
  private String using;
  private int tagNum;
  private List<Tag> tags;
  private List<ChildColRef> refs;
  private List<ChildTableInfo> createList;

  public String getUsing() {
    return using;
  }

  public void setUsing(String using) {
    this.using = using;
  }

  public int getTagNum() {
    return tagNum;
  }

  public void setTagNum(int tagNum) {
    this.tagNum = tagNum;
  }

  public List<Tag> getTags() {
    return tags;
  }

  public void setTags(List<Tag> tags) {
    this.tags = tags;
  }

  public List<ChildColRef> getRefs() {
    return refs;
  }

  public void setRefs(List<ChildColRef> refs) {
    this.refs = refs;
  }

  public List<ChildTableInfo> getCreateList() {
    return createList;
  }

  public void setCreateList(List<ChildTableInfo> createList) {
    this.createList = createList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MetaCreateChildTable that = (MetaCreateChildTable) o;
    return tagNum == that.tagNum
        && Objects.equals(using, that.using)
        && Objects.equals(tags, that.tags)
        && Objects.equals(refs, that.refs)
        && Objects.equals(createList, that.createList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), using, tagNum, tags, refs, createList);
  }
}
