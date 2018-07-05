package raft.server.storage;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Manifest {
    private final List<SSTableFileMetaInfo> metas;
    private final String baseDir;
    private final String storageName;

    private int nextFileNumber = 1;
    private LogWriter manifestRecordWriter;
    private int manifestFileNumber;

    Manifest(String baseDir, String storageName) {
        this.baseDir = baseDir;
        this.storageName = storageName;
        this.metas = new ArrayList<>();
    }

    void registerMeta(SSTableFileMetaInfo meta) {
        metas.add(meta);
    }

    void logRecord(ManifestRecord record) throws IOException {
        record.setNextFileNumber(nextFileNumber);

        String manifestFileName = null;
        if (manifestRecordWriter == null) {
            manifestFileNumber = getNextFileNumber();
            manifestFileName = FileName.getManifestFileName(storageName, manifestFileNumber);
            FileChannel manifestFile = FileChannel.open(Paths.get(baseDir, manifestFileName),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
            manifestRecordWriter = new LogWriter(manifestFile);
        }

        manifestRecordWriter.append(record.encode());

        if (manifestFileName != null) {
            FileName.setCurrentFile(baseDir, storageName, manifestFileNumber);
        }
    }

    void recover() {

    }

    synchronized int getNextFileNumber() {
        return nextFileNumber++;
    }

    /**
     * find all the SSTableFileMetaInfo who's index range intersect with startIndex and endIndex
     *
     * @param startKey target start key (inclusive)
     * @param endKey target end key (exclusive)
     *
     * @return iterator for found SSTableFileMetaInfo
     */
    Iterator<SSTableFileMetaInfo> searchMetas(int startKey, int endKey) {
        int startMetaIndex;
        if (metas.size() > 32) {
            startMetaIndex = binarySearchStartMeta(startKey);
        } else {
            startMetaIndex = traverseSearchStartMeta(startKey);
        }

        return new Iterator<SSTableFileMetaInfo>() {
            private int index = startMetaIndex;

            @Override
            public boolean hasNext() {
                if (index < metas.size()) {
                    SSTableFileMetaInfo meta = metas.get(index);
                    return meta.getFirstKey() < endKey;
                }
                return false;
            }

            @Override
            public SSTableFileMetaInfo next() {
                return metas.get(index++);
            }
        };
    }

    private int traverseSearchStartMeta(int index) {
        int i = 0;
        while (i < metas.size()) {
            SSTableFileMetaInfo meta = metas.get(i);
            if (index <= meta.getFirstKey()) {
                break;
            } else if (index <= meta.getLastKey()) {
                break;
            }
            ++i;
        }

        return i;
    }

    private int binarySearchStartMeta(int index) {
        int start = 0;
        int end = metas.size();

        while (start < end) {
            int mid = (start + end) / 2;
            SSTableFileMetaInfo meta = metas.get(mid);
            if (index >= meta.getFirstKey() && index <= meta.getLastKey()) {
                return mid;
            } else if (index < meta.getFirstKey()) {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        return start;
    }
}
