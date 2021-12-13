package com.taosdata.jdbc.annotation;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * to collect test information
 */
public class TestCatalogGenerater {

    private static ClassLoader cl;

    @Test
    public void doTest() throws IOException, ClassNotFoundException {
        List<Catalog> catalogList = new ArrayList<>();
        doScan("com.taosdata.jdbc", catalogList);
        if (catalogList.size() > 0) {
            try (FileWriter writer = new FileWriter("target/TestCaseCatalog.txt")) {
                writer.write("function\tmethod\tcondition\tauthor\tsince\n");
                for (Catalog catalog : catalogList) {
                    for (EachMethod method : catalog.methods) {
                        writer.write(catalog.function + "\t");
                        writer.write(method.message + "\t");
                        if (method.condition != null && method.condition.length() > 0) {
                            writer.write(method.condition);
                        }
                        writer.write("\t");
                        if (method.author != null && method.author.length() > 0) {
                            writer.write(method.author);
                        }
                        writer.write("\t");
                        if (method.version != null && method.version.length() > 0) {
                            writer.write(method.version);
                        }
                        writer.write("\n");
                    }
                    writer.write("------------------------------");
                }
            }
        }
    }

    private List<Catalog> doScan(String basePackage, List<Catalog> catalogList) throws IOException, ClassNotFoundException {
        if (null == basePackage || basePackage.trim().equals(""))
            return catalogList;
        String splashPath = dotToSplash(basePackage);
        URL url = cl.getResource(splashPath);
        if (null == url)
            return catalogList;
        String filePath = getRootPath(url);
        List<String> names = null;
        if (isJarFile(filePath)) {
            names = readFromJarFile(filePath, splashPath);
        } else {
            names = readFromDirectory(filePath);
        }
        if (null == names || names.isEmpty())
            return catalogList;
        for (String name : names) {
            if (isClassFile(name)) {
                Class<?> clazz = Class.forName(basePackage + "." + trimExtension(name));
                if (clazz.isAnnotationPresent(TestTarget.class)) {
                    TestTarget target = clazz.getAnnotation(TestTarget.class);
                    Catalog catalog = new Catalog();
                    catalog.function = target.value();
                    Method[] methods = getDeclaredMethods(clazz);
                    for (Method method : methods) {
                        if (method.isAnnotationPresent(Description.class)) {
                            Description annotation = method.getAnnotation(Description.class);
                            EachMethod eachMethod = new EachMethod();
                            eachMethod.message = annotation.message();
                            eachMethod.condition = annotation.condition();
                            if (null == annotation.author() || annotation.author().trim().length() < 1) {
                                eachMethod.author = target.author();
                            } else {
                                eachMethod.author = annotation.author();
                            }
                            if (null == annotation.version() || annotation.version().trim().length() < 1) {
                                eachMethod.version = target.version();
                            } else {
                                eachMethod.version = annotation.version();
                            }
                            catalog.methods.add(eachMethod);
                        }
                    }
                    if (catalog.methods.size() > 0) {
                        catalogList.add(catalog);
                    }
                }
            } else {
                doScan(basePackage + "." + name, catalogList);
            }
        }
        return catalogList;
    }

    private List<String> readFromJarFile(String jarPath, String splashedPackageName) throws IOException {
        JarInputStream jarIn = new JarInputStream(new FileInputStream(jarPath));
        JarEntry entry = jarIn.getNextJarEntry();

        List<String> nameList = new ArrayList<>();
        while (null != entry) {
            String name = entry.getName();
            if (name.startsWith(splashedPackageName) && isClassFile(name)) {
                nameList.add(name);
            }

            entry = jarIn.getNextJarEntry();
        }

        return nameList;
    }

    private static Method[] getDeclaredMethods(Class<?> clazz) {
        if (clazz == null)
            throw new IllegalArgumentException("Class must not be null");
        try {
            return clazz.getDeclaredMethods();
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to introspect Class [" + clazz.getName() +
                    "] from ClassLoader [" + clazz.getClassLoader() + "]", ex);
        }
    }

    private List<String> readFromDirectory(String path) {
        File file = new File(path);
        String[] names = file.list();

        if (null == names) {
            return null;
        }
        return Arrays.asList(names);
    }

    /**
     * "file:/home/whf/cn/fh" -> "/home/whf/cn/fh"
     * "jar:file:/home/whf/foo.jar!cn/fh" -> "/home/whf/foo.jar"
     */
    public static String getRootPath(URL url) {
        String fileUrl = url.getFile();
        int pos = fileUrl.indexOf('!');

        if (-1 == pos) {
            return fileUrl;
        }
        return fileUrl.substring(5, pos);
    }

    /**
     * "com.taosdata.jdbc" -> "com/taosdata/jdbc"
     */
    public static String dotToSplash(String name) {
        return name.replaceAll("\\.", File.separator);
    }

    private boolean isClassFile(String name) {
        return name.endsWith(".class");
    }

    private boolean isJarFile(String name) {
        return name.endsWith(".jar");
    }

    /**
     * Convert short class name to fully qualified name.
     * e.g., String -> java.lang.String
     */
    private String toFullyQualifiedName(String shortName, String basePackage) {
        StringBuilder sb = new StringBuilder(basePackage);
        sb.append('.');
        sb.append(trimExtension(shortName));

        return sb.toString();
    }

    /**
     * "Apple.class" -> "Apple"
     */
    public static String trimExtension(String name) {
        int pos = name.indexOf('.');
        if (-1 != pos) {
            return name.substring(0, pos);
        }
        return name;
    }

    @BeforeClass
    public static void beforeClass() {
        cl = Thread.currentThread().getContextClassLoader();
    }


    class Catalog {
        private String function;
        private List<EachMethod> methods = new ArrayList<>();
    }

    class EachMethod {
        private String message;
        private String condition;
        private String author;
        private String version;
    }
}