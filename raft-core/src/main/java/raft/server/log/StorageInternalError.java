package raft.server.log;

/**
 * Author: ylgrgyq
 * Date: 18/7/3
 */
public class StorageInternalError extends RuntimeException{
    private static final long serialVersionUID = -4360183127324133031L;

    public StorageInternalError(String s) {
        super(s);
    }

    public StorageInternalError(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageInternalError(Throwable cause) {
        super(cause);
    }
}
