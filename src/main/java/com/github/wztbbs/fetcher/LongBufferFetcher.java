package com.github.wztbbs.fetcher;

import java.nio.ByteBuffer;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class LongBufferFetcher implements BufferedCache {

    @Override
    public Long fetchObject(ByteBuffer buf, int position) {
        return buf.getLong(position);
    }

    @Override
    public void add(ByteBuffer buf, int position, ByteBuffer buf2) {
        long previous = buf.getLong(position);
        long value = buf2.getLong(position);
        buf.putLong(position, previous + value);
    }
}
