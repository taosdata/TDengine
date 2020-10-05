/*
 * Copyright Copyright 2020-present Marvin Liao <coldljy@163.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.extensions.taosdata.configuration;

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.List;

/**
 * 配置辅助工具
 */
public class CfgUtils {
    private static final Logger log = LoggerFactory.getLogger(CfgUtils.class);

    /**
     * 加载配置文件
     * @return 配置项
     */
    public static Config load() {
        return loadFromFile(Defs.DEFAULT_TOPICS_CFG_PATH);
    }

    public static Config loadFromFile(String filename) {
        Yaml yaml = new Yaml();
        File f = new File(filename);
        BufferedReader reader = null;

        try {
            reader = Files.newReader(f, Defs.UTF8);
            Config topics = yaml.loadAs(reader, Config.class);
            return topics;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log.error("failed to load topics configuration, file {}", filename, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                }
            }
        }

        return null;
    }

    public String readFile(String filename) {
        File f = new File(filename);
        List<String> lines = null;
        try {
            lines = Files.readLines(f, Defs.UTF8);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("failed to read topics configuration file {}", filename, e);
            return null;
        }

        StringBuilder sb = new StringBuilder();
        lines.forEach(line -> {
            sb.append(line + "\n");
        });

        return sb.toString();
    }
}
