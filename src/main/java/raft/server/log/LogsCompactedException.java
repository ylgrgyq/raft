package raft.server.log;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public class LogsCompactedException extends RuntimeException {

    private static final long serialVersionUID = 6345804997282967172L;

    public LogsCompactedException() {
        super();
    }

    public LogsCompactedException(String s) {
        super(s);
    }

    public LogsCompactedException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogsCompactedException(Throwable cause) {
        super(cause);
    }
}
