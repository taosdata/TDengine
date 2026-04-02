package com.taosdata.jdbc.annotation;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class
 */
public class CatalogClass {

    private String name;
    private String alias;
    private String author;
    private String version;
    private List<CatalogMethod> methods = new ArrayList<>();
    private int total;
    private int failure;

    public void setTotal(int total) {
        this.total = total;
    }

    public void setFailure(int failure) {
        this.failure = failure;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setMethods(List<CatalogMethod> methods) {
        this.methods = methods;
    }

    @Override
    public String toString() {
        if (methods.size() < 1)
            return "";
        StringBuilder sb = new StringBuilder();
        sb.append("ClassName: ").append(name);
        String msg = trim(alias);
        if (null != msg) {
            sb.append("\tAlias:").append(alias);
            sb.append("\tTotal:").append(total)
                    .append("\tFailure:").append(failure).append("\n");
        }
        for (CatalogMethod method : methods) {
            sb.append("\t").append(method.getName());
            sb.append("\t").append(method.isSuccess());
            sb.append("\t").append(method.getMessage());
            String mAuthor = trim(method.getAuthor());
            if (null == mAuthor) {
                sb.append("\t").append(author);
            } else {
                sb.append("\t").append(method.getAuthor());
            }
            String mVersion = trim(method.getVersion());
            if (null == mVersion) {
                sb.append("\t").append(version);
            } else {
                sb.append("\t").append(mVersion);
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String trim(String s) {
        if (null == s || s.trim().equals("")) {
            return null;
        } else {
            return s.trim();
        }
    }
}
