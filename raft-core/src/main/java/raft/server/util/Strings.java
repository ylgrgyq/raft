package raft.server.util;

public final class Strings {
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

    private Strings () { }
}
