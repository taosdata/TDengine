package com.taosdata.utils;

import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * 文件工具类
 *
 * @author ZYP
 */
public class FileUtils {

    /**
     * 读取相对路径文件
     *
     * @return
     */
    public static String readResourceFile(String relativePath, String fileName) throws Exception {
        // 获取jar同级目录
        String path = ResourceUtils.getURL("classpath:").getPath();
        // 目标文件
        File file = new File(path + File.separator + relativePath + File.separator + fileName);
        // 读取内容
        FileReader fileReader = new FileReader(file);
        // 定义一个相同长度的字符数组
        char[] chars = new char[(int) file.length()];
        // 读取文件内容
        fileReader.read(chars);
        // 转换为字符串并返回
        return new String(chars);
    }

    /**
     * 写入相对路径文件
     *
     * @param relativePath
     * @param fileName
     * @param content
     * @throws Exception
     */
    public static void writeResourceFile(String relativePath, String fileName, String content) throws Exception {
        // 获取jar同级目录
        String path = ResourceUtils.getURL("classpath:").getPath();
        // 文件目录
        File dir = new File(path + File.separator + relativePath);
        // 递归创建目录（已存在则默认忽略）
        dir.mkdirs();
        // 目标文件
        File file = new File(path + File.separator + relativePath + File.separator + fileName);
        // 不存在则新建
        if (!file.exists()) {
            file.createNewFile();
        }
        // 写入内容
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(content);
        fileWriter.flush();
    }
}
