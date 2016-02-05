package com.github.wztbbs.query;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.github.wztbbs.fetcher.BufferedCache;
import com.github.wztbbs.fetcher.CardinalityBufferFetcher;
import com.github.wztbbs.fetcher.DoubleBufferFetcher;
import com.github.wztbbs.fetcher.LongBufferFetcher;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.*;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQueryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class GroupParallelQueryRunner<T> implements QueryRunner<T> {

    private static final Logger logger = LoggerFactory.getLogger(GroupParallelQueryRunner.class);

    private Iterable<QueryRunner> queryables;

    private ExecutorService executorService;

    private Supplier<GroupByQueryConfig> configSupplier;

    private Map<Class, BufferedCache> fetcherMap;


    @Inject
    public GroupParallelQueryRunner(Iterable<QueryRunner> queryables, ExecutorService executorService, Supplier<GroupByQueryConfig> configSupplier) {
        this.queryables = queryables;
        this.executorService = executorService;
        this.configSupplier = configSupplier;
        fetcherMap = new HashMap<>();
        CardinalityBufferFetcher cardinalityBufferFetcher = new CardinalityBufferFetcher();
        DoubleBufferFetcher doubleBufferFetch = new DoubleBufferFetcher();
        LongBufferFetcher longBufferFetch = new LongBufferFetcher();
        fetcherMap.put(CardinalityAggregatorFactory.class, cardinalityBufferFetcher);
        fetcherMap.put(CountAggregatorFactory.class, cardinalityBufferFetcher);
        fetcherMap.put(DoubleMaxAggregatorFactory.class, doubleBufferFetch);
        fetcherMap.put(DoubleMinAggregatorFactory.class, doubleBufferFetch);
        fetcherMap.put(DoubleSumAggregatorFactory.class, doubleBufferFetch);
        fetcherMap.put(MaxAggregatorFactory.class, doubleBufferFetch);
        fetcherMap.put(MinAggregatorFactory.class, doubleBufferFetch);
        fetcherMap.put(LongMaxAggregatorFactory.class, longBufferFetch);
        fetcherMap.put(LongMinAggregatorFactory.class, longBufferFetch);
        fetcherMap.put(LongSumAggregatorFactory.class, longBufferFetch);
    }

    @Override
    public Sequence<T> run(Query queryParam, Map responseContext) {

        GroupQuery query = (GroupQuery) queryParam;
        final Pair<MapWrap<String, ByteBuffer>, Accumulator<MapWrap<String, ByteBuffer>, Map.Entry<String, ByteBuffer>>> indexAccumulatorPair = createIndexAccumulatorPair(
                query,
                configSupplier.get()
        );

        Function<QueryRunner, Future<Void>> function = new Function<QueryRunner, Future<Void>>() {
            @Override
            public Future<Void> apply(final QueryRunner input) {
                if (input == null) {
                    throw new ISE("Null queryRunner! Looks to be some segment unmapping action happening");
                }

                Callable<Void> callable = new AbstractPrioritizedCallable<Void>(0) {
                    @Override
                    public Void call() throws Exception {
                        try {
                            input.run(query, responseContext)
                                    .accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
                            return null;
                        } catch (QueryInterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (Exception e) {
                            logger.error("Exception with one of the sequences!, e:", e);
                            throw Throwables.propagate(e);
                        }
                    }
                };

                return executorService.submit(callable);
            };
        };

        List<Future> list = Lists.newArrayList(Iterables.transform(queryables, function));
        for(Future future : list) {
            try {
                future.get();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.info("limit begin..., limit:{}, having:{}", query.getLimitSpec(), query.getHavingSpec());
        Sequence<Row> sequence = query.applyLimit(entry2Row(query, indexAccumulatorPair.lhs));
        logger.info("limit end...");

        return Sequences.map(sequence, new Function<Row, T>() {

            int number = 0;

            @Nullable
            @Override
            public T apply(@Nullable Row input) {
                if(number ++ < 10) {
                    logger.info("key:{}, total hits:{}",input.getDimension("ip"),input.getLongMetric("totalHits"));
                }
                return (T)input;
            }
        });
    }

    private Sequence<Row> entry2Row(GroupQuery query, MapWrap mapWrap) {
        List<DimensionSpec> dims = query.getDimensions();
        List<AggregatorFactory> aggregators = query.getAggregatorSpecs();

        return Sequences.map(
                Sequences.simple(mapWrap.entrySet()),
                new Function<Map.Entry, Row>() {

                    @Override
                    public Row apply(Map.Entry input) {
                        Map<String, Object> theEvent = Maps.newLinkedHashMap();
                        String[] keys = ((String)input.getKey()).split(GroupQueryRunner.SEPARATOR);
                        if(keys.length != dims.size() + 1) {
                            throw new RuntimeException("size is not correct, key: " + input.getKey());
                        }
                        long time = Long.parseLong(keys[0]);
                        for(int i = 1; i < keys.length; i++) {
                            theEvent.put(dims.get(i-1).getDimension(), keys[i]);
                        }

                        int position = 0;
                        for(int i = 0; i < aggregators.size(); i++) {
                            AggregatorFactory aggregatorFactory = aggregators.get(i);
                            theEvent.put(aggregatorFactory.getName(), fetcherMap.get(aggregatorFactory.getClass()).fetchObject((ByteBuffer)input.getValue(), position));
                            position += aggregatorFactory.getMaxIntermediateSize();
                        }

                        return new MapBasedRow(time, theEvent);
                    }
                }
        );
    }

    public <T> Pair<MapWrap<String, ByteBuffer>, Accumulator<MapWrap<String, ByteBuffer>, Map.Entry<String,ByteBuffer>>> createIndexAccumulatorPair(
            final GroupQuery query,
            final GroupByQueryConfig config
    )
    {
        List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
        int[] sizesRequired = new int[aggregatorSpecs.size()];

        int capacity = 100000;
        for(int i = 0; i < aggregatorSpecs.size(); ++i) {
            AggregatorFactory factory = aggregatorSpecs.get(i);
            sizesRequired[i] = factory.getMaxIntermediateSize();
        }

        MapWrap mapWrap = new MapWrap(capacity);
        Accumulator<MapWrap<String, ByteBuffer>, Map.Entry<String,ByteBuffer>> accumulator = new Accumulator<MapWrap<String, ByteBuffer>, Map.Entry<String,ByteBuffer>>()
        {

            @Override
            public MapWrap accumulate(MapWrap<String, ByteBuffer> accumulated, Map.Entry<String,ByteBuffer> in)
            {

                try {
                    Map.Entry entry = (Map.Entry) in;
                    String key = (String)entry.getKey();
                    ByteBuffer value = (ByteBuffer)entry.getValue();

                    Object current = accumulated.putIfAbsent(key, value);
                    if(current != null) {
                        synchronized (current) {
                            ByteBuffer currentBuffer = (ByteBuffer)current;
                            int position = 0;
                            for(int i=0; i < aggregatorSpecs.size(); i++) {
                                AggregatorFactory factory = aggregatorSpecs.get(i);
                                BufferedCache fetcher = fetcherMap.get(factory.getClass());
                                fetcher.add(currentBuffer, position, value);
                                position += sizesRequired[i];
                            }
                        }
                    }
                }
                catch (Exception e) {
                    throw new ISE(e.getMessage());
                }

                return accumulated;
            }
        };
        return new Pair<MapWrap<String, ByteBuffer>, Accumulator<MapWrap<String, ByteBuffer>, Map.Entry<String,ByteBuffer>>>(mapWrap, accumulator);
    }
}
