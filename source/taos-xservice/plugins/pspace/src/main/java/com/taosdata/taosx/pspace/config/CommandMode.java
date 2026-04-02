package com.taosdata.taosx.pspace.config;

import java.util.Arrays;
import java.util.List;

/**
 * Centralized mode candidates for the taosx-pspace CLI.
 *
 * Keeps allowed modes in one place so they can be extended easily.
 */
public class CommandMode implements Iterable<String> {
    public static final String[] values = new String[] {
            "check",
            "nodes",
            "points",
            "run"
    };

    public static boolean isValid(String v) {
        if (v == null)
            return false;
        for (String s : values)
            if (s.equals(v))
                return true;
        return false;
    }

    // picocli will use this for tab-completion in supported shells
    public static List<String> list() {
        return Arrays.asList(values);
    }

    @Override
    public java.util.Iterator<String> iterator() {
        return Arrays.asList(values).iterator();
    }
}
