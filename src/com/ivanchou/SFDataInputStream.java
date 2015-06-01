package com.ivanchou;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created by ivanchou on 6/1/15.
 */
public class SFDataInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {
    public SFDataInputStream(byte[] bytes) {
        super(bytes);
    }

    public SFDataInputStream(byte[] bytes, int i, int i1) {
        super(bytes, i, i1);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos < 0 || pos > count) {
            throw new IndexOutOfBoundsException("" + pos + ":" + count);
        }
        this.pos = (int) pos;
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i1) throws IOException {
        return 0;
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {

    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {

    }
}
