package com.github.wztbbs.fetcher;

import java.nio.ByteBuffer;

/**
 * Created by wztbbs on 2015/10/9.
 */
public interface BufferedCache {

    public Object fetchObject(ByteBuffer buf, int position);

    public void add(ByteBuffer buf, int position, ByteBuffer buf2);
}
