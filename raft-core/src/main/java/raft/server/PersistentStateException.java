package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/4/25
 */
public class PersistentStateException extends RuntimeException {
    static final long serialVersionUID = 722444630034251875L;

    public PersistentStateException() {
        super();
    }

    public PersistentStateException(String s) {
        super(s);
    }

    public PersistentStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public PersistentStateException(Throwable cause) {
        super(cause);
    }
}
