package com.github.wztbbs.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wztbbs on 2015/8/25.
 */
public class MapWrap<K, V> extends ConcurrentHashMap<K,V>  {

    public MapWrap() {
        super();
    }

    public MapWrap(int capacity) {
        super(capacity);
    }

}
