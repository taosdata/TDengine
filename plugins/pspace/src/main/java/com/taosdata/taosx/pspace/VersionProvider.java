package com.taosdata.taosx.pspace;

import picocli.CommandLine.IVersionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Provides dynamic version information: project version, git commit id and
 * build info.
 */
public class VersionProvider implements IVersionProvider {

    @Override
    public String[] getVersion() throws Exception {
        Properties v = loadProperties("version.properties");
        Properties g = loadProperties("git.properties");

        String version = safeGet(v, "version", "unknown");
        String buildTimestamp = safeGet(v, "build.timestamp", "unknown");
        String osName = safeGet(v, "build.os.name", System.getProperty("os.name", "unknown"));
        String osArch = safeGet(v, "build.os.arch", System.getProperty("os.arch", "unknown"));

        String mappedOs = mapOsName(osName);
        String mappedArch = mapArch(osArch);

        String gitCommit = safeGet(g, "git.commit.id", safeGet(g, "git.commit.id.abbrev", "unknown"));

        // Prefer git.build.time (produced by git-commit-id-plugin) because it contains
        // timezone info in local zone.
        String gitBuildTime = safeGet(g, "git.build.time", null);
        String formattedBuildTime = buildTimestamp;
        if (gitBuildTime != null && !gitBuildTime.equals("")) {
            // git.build.time is like 2026-01-20T18:11:07+0800 (no colon in offset)
            try {
                DateTimeFormatter in = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");
                OffsetDateTime odt = OffsetDateTime.parse(gitBuildTime, in);
                DateTimeFormatter out = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX");
                formattedBuildTime = odt.format(out);
            } catch (Exception ignored) {
                // fallback to original buildTimestamp if parsing fails
                formattedBuildTime = buildTimestamp;
            }
        }

        String versionLine = String.format("version: %s (core-%s debug)", version, version);
        String gitLine = String.format("git: %s", gitCommit);
        String buildLine = String.format("build: %s-%s %s", mappedOs, mappedArch, formattedBuildTime);

        return new String[] { versionLine, gitLine, buildLine };
    }

    private Properties loadProperties(String resource) throws IOException {
        Properties p = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
            if (in != null) {
                p.load(in);
            }
        }
        return p;
    }

    private String safeGet(Properties p, String key, String def) {
        String v = p.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? def : v.trim();
    }

    private String mapOsName(String osName) {
        String s = osName.toLowerCase();
        if (s.contains("mac") || s.contains("darwin"))
            return "macos";
        if (s.contains("win"))
            return "windows";
        if (s.contains("nux") || s.contains("nix") || s.contains("linux"))
            return "linux";
        return s.replaceAll("\\s+", "-");
    }

    private String mapArch(String arch) {
        String s = arch.toLowerCase();
        if (s.contains("aarch") || s.contains("arm64"))
            return "aarch64";
        if (s.contains("amd64") || s.contains("x86_64") || s.contains("x86-64"))
            return "x86_64";
        if (s.contains("x86") || s.contains("i386") || s.contains("i486") || s.contains("i686"))
            return "x86";
        return s.replaceAll("[^a-z0-9_-]", "");
    }
}
