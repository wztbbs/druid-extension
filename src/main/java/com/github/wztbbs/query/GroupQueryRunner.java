package com.github.wztbbs.query;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.*;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class GroupQueryRunner implements QueryRunner {

    private static final Logger logger = LoggerFactory.getLogger(GroupQueryRunner.class);

    public static final String SEPARATOR = "@#";

    private final Supplier<GroupByQueryConfig> config;

    private final StorageAdapter storageAdapter;


    @Inject
    public GroupQueryRunner(Supplier<GroupByQueryConfig> config, Segment segment) {
        this.config = config;
        this.storageAdapter = segment.asStorageAdapter();
    }

    @Override
    public Sequence run(Query queryParam, Map responseContext) {
        if (!(queryParam instanceof GroupQuery)) {
            throw new ISE("Got a [%s] which isn't a %s", queryParam.getClass(), GroupQuery.class);
        }

        GroupQuery query = (GroupQuery)queryParam;
        final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
        if (intervals.size() != 1) {
            throw new IAE("Should only have one interval, got[%s]", intervals);
        }

        final Sequence<Cursor> cursors = storageAdapter.makeCursors(
                Filters.convertDimensionFilters(query.getDimFilter()),
                intervals.get(0),
                query.getGranularity()
        );

        return Sequences.concat(
                Sequences.withBaggage(
                        Sequences.map(
                                cursors,
                                new Function<Cursor, Sequence<Map.Entry<String, ByteBuffer>>>() {
                                    @Override
                                    public Sequence<Map.Entry<String, ByteBuffer>> apply(final Cursor cursor) {
                                        return new BaseSequence<>(
                                                new GroupIteratorMaker(cursor, query)
                                        );
                                    }
                                }
                        ),
                        new Closeable() {
                            @Override
                            public void close() throws IOException {

                            }
                        }
                )
        );

    }

    public int getCardinality(String filedName) {
        return storageAdapter.getDimensionCardinality(filedName);
    }

    public  class GroupIteratorMaker implements BaseSequence.IteratorMaker<Map.Entry<String, ByteBuffer>, Iterator<Map.Entry<String, ByteBuffer>>> {

        private Cursor cursor;

        private GroupQuery query;

        public GroupIteratorMaker(Cursor cursor, GroupQuery query) {
            this.cursor = cursor;
            this.query = query;
        }

        @Override
        public Iterator<Map.Entry<String, ByteBuffer>> make() {

            long begin = System.currentTimeMillis();
            List<DimensionSelector> dimensions = new ArrayList<DimensionSelector>();
            List<BufferAggregator> aggregators = new ArrayList<BufferAggregator>();

            int maxSize = 0;
            List<DimensionSpec> dimensionSpecs = query.getDimensions();
            for(DimensionSpec dimSpec : dimensionSpecs) {
                DimensionSelector selector = cursor.makeDimensionSelector(dimSpec.getDimension(), dimSpec.getExtractionFn());
                dimensions.add(selector);
                int cardinality = storageAdapter.getDimensionCardinality(dimSpec.getDimension());
                maxSize = Math.max(cardinality, maxSize);
            }

            List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
            int[] sizesRequired = new int[aggregatorSpecs.size()];
            int size = 0;
            for(int i = 0; i < aggregatorSpecs.size(); ++i) {
                AggregatorFactory factory = aggregatorSpecs.get(i);
                size += factory.getMaxIntermediateSize();
                sizesRequired[i] = factory.getMaxIntermediateSize();
                BufferAggregator aggregator = factory.factorizeBuffered(cursor);
                aggregators.add(aggregator);
            }

            Integer number = 0;
            Map<ByteBuffer, ByteBuffer> tempMap = new HashMap<>(maxSize * 2);
            int keySize = dimensions.size() * Ints.BYTES + Longs.BYTES;

            while (!cursor.isDone()) {
                ByteBuffer key = ByteBuffer.allocate(keySize);
                ByteBuffer keyCopy = key.duplicate();

                keyCopy.putLong(cursor.getTime().getMillis());
                for(DimensionSelector selector : dimensions) {
                    keyCopy.putInt(selector.getRow().get(0));
                }

//                StringBuffer key = new StringBuffer();
//                key.append(cursor.getTime().getMillis()).append(SEPARATOR);
//                for(DimensionSelector selector : dimensions) {
//                    key.append(selector.lookupName(selector.getRow().get(0))).append(SEPARATOR);
//                }

                ByteBuffer byteBuffer = tempMap.get(key);
                if(byteBuffer == null) {
                    byteBuffer = ByteBuffer.allocateDirect(size);
                    tempMap.put(key, byteBuffer);
                }
                int position = 0;
                for(int i = 0; i < aggregatorSpecs.size(); ++i) {
                    aggregators.get(i).aggregate(byteBuffer, position);
                    position += sizesRequired[i];
                }
                cursor.advance();
                number++;
            }

            long end = System.currentTimeMillis();
            logger.info("finish a query runner, time cost is:{}, size is:{}",(end - begin), tempMap.size());
            logger.info("number is:{}, maxSize:{}",number, maxSize);

            Map<String,ByteBuffer> result = new HashMap<String, ByteBuffer>(tempMap.size() * 2);
            for(Map.Entry<ByteBuffer, ByteBuffer> entry : tempMap.entrySet()) {
                ByteBuffer key = entry.getKey();
                StringBuffer stringBuffer = new StringBuffer(key.getLong() + "").append(GroupQueryRunner.SEPARATOR);
                for(int i = 0; i < dimensions.size(); i++) {
                    DimensionSelector selector = dimensions.get(i);
                    stringBuffer.append(selector.lookupName(key.getInt())).append(GroupQueryRunner.SEPARATOR);
                }
                result.put(stringBuffer.toString(), entry.getValue());
            }
            return result.entrySet().iterator();
        }

        @Override
        public void cleanup(Iterator<Map.Entry<String, ByteBuffer>> iterFromMake) {

        }
    }


}
