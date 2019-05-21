package raft.server.util;

public final class Preconditions {
    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    public static <T> T checkNotNull(T reference, Object errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        } else {
            return reference;
        }
    }

    public static void checkArgument(boolean b) {
        if (!b) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean b, Object errorMessage) {
        if (!b) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean b, String errorMessageTemplate, Object ...args) {
        if (!b) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, args));
        }
    }

    public static void checkState(boolean b) {
        if (!b) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean b, Object errorMessage) {
        if (!b) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    public static void checkState(boolean b, String errorMessageTemplate, Object ...args) {
        if (!b) {
            throw new IllegalStateException(String.format(errorMessageTemplate, args));
        }
    }

    private Preconditions () {}
}
