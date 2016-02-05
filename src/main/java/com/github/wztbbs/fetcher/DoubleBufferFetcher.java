package com.github.wztbbs.fetcher;

import java.nio.ByteBuffer;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class DoubleBufferFetcher implements BufferedCache {
    @Override
    public Double fetchObject(ByteBuffer buf, int position) {
        return  buf.getDouble(position);
    }

    @Override
    public void add(ByteBuffer buf, int position, ByteBuffer buf2) {
        double previous = buf.getDouble(position);
        double value = buf2.getDouble(position);
        buf.putDouble(position, value + previous);
    }
}
