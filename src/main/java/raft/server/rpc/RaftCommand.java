package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public abstract class RaftCommand implements SerializableCommand {
    private CommandCode code;

    RaftCommand(){
    }

    RaftCommand(CommandCode code) {
        this.code = code;
    }

    CommandCode getCommandCode() {
        return this.code;
    }

    void setCode(CommandCode code) {
        this.code = code;
    }
}
