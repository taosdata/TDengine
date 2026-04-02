package com.taosdata.jdbc.annotation;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.io.File;
import java.io.FileWriter;
import java.util.LinkedList;

public class CatalogListener extends RunListener {
    public static final String CATALOG_FILE = "target/TestCaseCatalog.txt";
    CatalogClass catalogClass = null;
    private final LinkedList<CatalogMethod> methods = new LinkedList<>();

    @Override
    public void testRunStarted(Description description) throws Exception {
        catalogClass = new CatalogClass();
        TestTarget target = description.getAnnotation(TestTarget.class);
        if (target != null) {
            catalogClass.setAlias(target.alias());
            catalogClass.setAuthor(target.author());
            catalogClass.setVersion(target.version());
        }
        catalogClass.setName(getClassName(description.getClassName()));
    }

    private String getClassName(String name) {
        if (null == name || name.trim().equals("")) {
            return null;
        }
        name = name.trim();
        int pos = name.lastIndexOf(".");
        if (-1 == pos) {
            return name;
        }
        return name.substring(pos + 1);
    }

    @Override
    public void testRunFinished(Result result) throws Exception {
        catalogClass.setMethods(methods);
        catalogClass.setTotal(result.getRunCount());
        catalogClass.setFailure(result.getFailureCount());
        File file = new File(CATALOG_FILE);
        if (!file.exists()) {
            synchronized (CatalogListener.class) {
                if (!file.exists()) {
                    file.createNewFile();
                    try (FileWriter writer = new FileWriter(file, true)) {
                        writer.write("\tName\tPass\tMessage\tAuthor\tVersion\n");
                        writer.write(catalogClass.toString());
                    }
                }
            }
        } else {
            try (FileWriter writer = new FileWriter(file, true)) {
                writer.write(catalogClass.toString());
            }
        }
    }

    @Override
    public void testStarted(Description description) throws Exception {
    }

    @Override
    public void testFinished(Description description) throws Exception {
        com.taosdata.jdbc.annotation.Description annotation
                = description.getAnnotation(com.taosdata.jdbc.annotation.Description.class);
        if (annotation != null) {
            CatalogMethod method = new CatalogMethod();
            method.setMessage(annotation.value());
            method.setAuthor(annotation.author());
            method.setVersion(annotation.version());
            method.setSuccess(true);
            method.setName(description.getMethodName());
            methods.addLast(method);
        }
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        com.taosdata.jdbc.annotation.Description annotation
                = failure.getDescription().getAnnotation(com.taosdata.jdbc.annotation.Description.class);
        if (null == annotation)
            return;
        CatalogMethod method = new CatalogMethod();
        method.setMessage(annotation.value());
        method.setAuthor(annotation.author());
        method.setVersion(annotation.version());
        method.setSuccess(false);
        method.setName(failure.getDescription().getMethodName());
        methods.addFirst(method);
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
    }

    @Override
    public void testIgnored(Description description) throws Exception {
        super.testIgnored(description);
    }
}