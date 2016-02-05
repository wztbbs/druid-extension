package com.github.wztbbs.fetcher;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;

import java.nio.ByteBuffer;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class CardinalityBufferFetcher implements BufferedCache {

    private static final HashFunction hashFn = Hashing.murmur3_128();

    @Override
    public HyperLogLogCollector fetchObject(ByteBuffer byteBuffer, int position) {
        ByteBuffer dataCopyBuffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
        ByteBuffer mutationBuffer = byteBuffer.duplicate();
        mutationBuffer.position(position);
        mutationBuffer.get(dataCopyBuffer.array());
        return HyperLogLogCollector.makeCollector(dataCopyBuffer);
    }

    @Override
    public void add(ByteBuffer buf, int position, ByteBuffer buf2) {
        final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
                (ByteBuffer) buf.duplicate().position(position).limit(
                        position
                                + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
                )
        );


        collector.fold((ByteBuffer) buf2.duplicate().position(position).limit(
                position + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
        ));
    }
}
