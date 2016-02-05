package com.github.wztbbs.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.*;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.annotations.Global;
import io.druid.query.*;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class GroupToolChest extends QueryToolChest<Row, GroupQuery> {

    private static final byte GROUPBY_QUERY = 0x15;
    private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
            new TypeReference<Object>()
            {
            };
    private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
    {
    };
    private static final String GROUP_BY_MERGE_KEY = "groupByMerge";

    private final Supplier<GroupByQueryConfig> configSupplier;

    private final StupidPool<ByteBuffer> bufferPool;
    private final ObjectMapper jsonMapper;

    private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

    @Inject
    public GroupToolChest(
            Supplier<GroupByQueryConfig> configSupplier,
            ObjectMapper jsonMapper,
            @Global StupidPool<ByteBuffer> bufferPool,
            IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
    )
    {
        this.configSupplier = configSupplier;
        this.jsonMapper = jsonMapper;
        this.bufferPool = bufferPool;
        this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    }

    @Override
    public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
    {
        return new QueryRunner<Row>()
        {
            @Override
            public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
            {
                if (input.getContextBySegment(false)) {
                    return runner.run(input, responseContext);
                }

                if (Boolean.valueOf(input.getContextValue(GROUP_BY_MERGE_KEY, "true"))) {
                    return mergeGroupByResults(
                            (GroupQuery) input,
                            runner,
                            responseContext
                    );
                }
                return runner.run(input, responseContext);
            }
        };
    }

    private Sequence<Row> mergeGroupByResults(
            final GroupQuery query,
            QueryRunner<Row> runner,
            Map<String, Object> context
    )
    {
        // If there's a subquery, merge subquery results and then apply the aggregator
        GroupQuery overrideQuery = query;
        boolean containsHashKey = query.isContainsHashKey();
        if(!containsHashKey) {
            overrideQuery = new GroupQuery(
                            query.getDataSource(),
                            query.getQuerySegmentSpec(),
                            query.getDimFilter(),
                            query.getGranularity(),
                            query.getDimensions(),
                            query.isContainsHashKey(),
                            query.getAggregatorSpecs(),
                            // Don't do post aggs until the end of this method.
                            ImmutableList.<PostAggregator>of(),
                            // Don't do "having" clause until the end of this method.
                            null,
                            null,
                            query.getContext()
                    ).withOverriddenContext(
                            ImmutableMap.<String, Object>of(
                                    "finalize", false
                            )
                    );
        }
        final DataSource dataSource = query.getDataSource();
        final IncrementalIndex index = makeIncrementalIndex(
                query, runner.run(
                        overrideQuery
                        , context
                )
        );
        return new ResourceClosingSequence<>(query.applyLimit(postAggregate(query, index)), index);
    }


    private Sequence<Row> postAggregate(final GroupQuery query, IncrementalIndex index)
    {
        return Sequences.map(
                Sequences.simple(index.iterableWithPostAggregations(query.getPostAggregatorSpecs())),
                new Function<Row, Row>() {
                    @Override
                    public Row apply(Row input) {
                        final MapBasedRow row = (MapBasedRow) input;
                        return new MapBasedRow(
                                query.getGranularity()
                                        .toDateTime(row.getTimestampFromEpoch()),
                                row.getEvent()
                        );
                    }
                }
        );
    }

    private IncrementalIndex makeIncrementalIndex(GroupQuery query, Sequence<Row> rows)
    {
        final GroupByQueryConfig config = configSupplier.get();
        Pair<IncrementalIndex, Accumulator<IncrementalIndex, Row>> indexAccumulatorPair = GroupQueryHelper.createIndexAccumulatorPair(
                query,
                config,
                bufferPool
        );

        return rows.accumulate(indexAccumulatorPair.lhs, indexAccumulatorPair.rhs);
    }

    @Override
    public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
    {
        return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
    }

    @Override
    public Sequence<Row> mergeSequencesUnordered(Sequence<Sequence<Row>> seqOfSequences)
    {
        return new MergeSequence<>(getOrdering(), seqOfSequences);
    }

    private Ordering<Row> getOrdering()
    {
        return Ordering.<Row>natural().nullsFirst();
    }

    @Override
    public ServiceMetricEvent.Builder makeMetricBuilder(GroupQuery query)
    {
        return DruidMetrics.makePartialQueryTimeMetric(query)
                .setDimension("numDimensions", String.valueOf(query.getDimensions().size()))
                .setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()))
                .setDimension(
                        "numComplexMetrics",
                        String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                );
    }

    @Override
    public Function<Row, Row> makePreComputeManipulatorFn(
            final GroupQuery query,
            final MetricManipulationFn fn
    )
    {
        return new Function<Row, Row>()
        {
            @Override
            public Row apply(Row input)
            {
                if (input instanceof MapBasedRow) {
                    final MapBasedRow inputRow = (MapBasedRow) input;
                    final Map<String, Object> values = Maps.newHashMap(inputRow.getEvent());
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                        values.put(agg.getName(), fn.manipulate(agg, inputRow.getEvent().get(agg.getName())));
                    }
                    return new MapBasedRow(inputRow.getTimestamp(), values);
                }
                return input;
            }
        };
    }

    @Override
    public Function<Row, Row> makePostComputeManipulatorFn(
            final GroupQuery query,
            final MetricManipulationFn fn
    )
    {
        final Set<String> optimizedDims = ImmutableSet.copyOf(
                Iterables.transform(
                        extractionsToRewrite(query),
                        new Function<DimensionSpec, String>()
                        {
                            @Override
                            public String apply(DimensionSpec input)
                            {
                                return input.getOutputName();
                            }
                        }
                )
        );
        final Function<Row, Row> preCompute = makePreComputeManipulatorFn(query, fn);
        if (optimizedDims.isEmpty()) {
            return preCompute;
        }

        // If we have optimizations that can be done at this level, we apply them here

        final Map<String, ExtractionFn> extractionFnMap = new HashMap<String, ExtractionFn>();
        for (DimensionSpec dimensionSpec : query.getDimensions()) {
            final String dimension = dimensionSpec.getOutputName();
            if (optimizedDims.contains(dimension)) {
                extractionFnMap.put(dimension, dimensionSpec.getExtractionFn());
            }
        }

        return new Function<Row, Row>()
        {
            @Nullable
            @Override
            public Row apply(Row input)
            {
                Row preRow = preCompute.apply(input);
                if (preRow instanceof MapBasedRow) {
                    MapBasedRow preMapRow = (MapBasedRow) preRow;
                    Map<String, Object> event = Maps.newHashMap(preMapRow.getEvent());
                    for (String dim : optimizedDims) {
                        final Object eventVal = event.get(dim);
                        event.put(dim, extractionFnMap.get(dim).apply(eventVal));
                    }
                    return new MapBasedRow(preMapRow.getTimestamp(), event);
                } else {
                    return preRow;
                }
            }
        };
    }

    @Override
    public TypeReference<Row> getResultTypeReference()
    {
        return TYPE_REFERENCE;
    }

    @Override
    public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
    {
        return new SubqueryQueryRunner<>(
                intervalChunkingQueryRunnerDecorator.decorate(
                        new QueryRunner<Row>()
                        {
                            @Override
                            public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
                            {
                                if (!(query instanceof GroupQuery)) {
                                    return runner.run(query, responseContext);
                                }
                                GroupQuery groupQuery = (GroupQuery) query;
                                ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
                                Set<String> optimizedDimensions = ImmutableSet.copyOf(
                                        Iterables.transform(
                                                extractionsToRewrite(groupQuery),
                                                new Function<DimensionSpec, String>()
                                                {
                                                    @Override
                                                    public String apply(DimensionSpec input)
                                                    {
                                                        return input.getDimension();
                                                    }
                                                }
                                        )
                                );
                                for (DimensionSpec dimensionSpec : groupQuery.getDimensions()) {
                                    if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                                        dimensionSpecs.add(
                                                new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                                        );
                                    } else {
                                        dimensionSpecs.add(dimensionSpec);
                                    }
                                }
                                return runner.run(
                                        groupQuery,
                                        responseContext
                                );
                            }
                        }, this
                )
        );
    }

    @Override
    public CacheStrategy<Row, Object, GroupQuery> getCacheStrategy(final GroupQuery query)
    {
        return new CacheStrategy<Row, Object, GroupQuery>()
        {
            private static final byte CACHE_STRATEGY_VERSION = 0x1;
            private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
            private final List<DimensionSpec> dims = query.getDimensions();


            @Override
            public byte[] computeCacheKey(GroupQuery query)
            {
                final DimFilter dimFilter = query.getDimFilter();
                final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
                final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
                final byte[] granularityBytes = query.getGranularity().cacheKey();
                final byte[][] dimensionsBytes = new byte[query.getDimensions().size()][];
                int dimensionsBytesSize = 0;
                int index = 0;
                for (DimensionSpec dimension : query.getDimensions()) {
                    dimensionsBytes[index] = dimension.getCacheKey();
                    dimensionsBytesSize += dimensionsBytes[index].length;
                    ++index;
                }
                final byte[] havingBytes = query.getHavingSpec() == null ? new byte[]{} : query.getHavingSpec().getCacheKey();
                final byte[] limitBytes = query.getLimitSpec().getCacheKey();

                ByteBuffer buffer = ByteBuffer
                        .allocate(
                                2
                                        + granularityBytes.length
                                        + filterBytes.length
                                        + aggregatorBytes.length
                                        + dimensionsBytesSize
                                        + havingBytes.length
                                        + limitBytes.length
                        )
                        .put(GROUPBY_QUERY)
                        .put(CACHE_STRATEGY_VERSION)
                        .put(granularityBytes)
                        .put(filterBytes)
                        .put(aggregatorBytes);

                for (byte[] dimensionsByte : dimensionsBytes) {
                    buffer.put(dimensionsByte);
                }

                return buffer
                        .put(havingBytes)
                        .put(limitBytes)
                        .array();
            }

            @Override
            public TypeReference<Object> getCacheObjectClazz()
            {
                return OBJECT_TYPE_REFERENCE;
            }

            @Override
            public Function<Row, Object> prepareForCache()
            {
                return new Function<Row, Object>()
                {
                    @Override
                    public Object apply(Row input)
                    {
                        if (input instanceof MapBasedRow) {
                            final MapBasedRow row = (MapBasedRow) input;
                            final List<Object> retVal = Lists.newArrayListWithCapacity(1 + dims.size() + aggs.size());
                            retVal.add(row.getTimestamp().getMillis());
                            Map<String, Object> event = row.getEvent();
                            for (DimensionSpec dim : dims) {
                                retVal.add(event.get(dim.getOutputName()));
                            }
                            for (AggregatorFactory agg : aggs) {
                                retVal.add(event.get(agg.getName()));
                            }
                            return retVal;
                        }

                        throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
                    }
                };
            }

            @Override
            public Function<Object, Row> pullFromCache()
            {
                return new Function<Object, Row>()
                {
                    private final QueryGranularity granularity = query.getGranularity();

                    @Override
                    public Row apply(Object input)
                    {
                        Iterator<Object> results = ((List<Object>) input).iterator();

                        DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

                        Map<String, Object> event = Maps.newLinkedHashMap();
                        Iterator<DimensionSpec> dimsIter = dims.iterator();
                        while (dimsIter.hasNext() && results.hasNext()) {
                            final DimensionSpec factory = dimsIter.next();
                            event.put(factory.getOutputName(), results.next());
                        }

                        Iterator<AggregatorFactory> aggsIter = aggs.iterator();
                        while (aggsIter.hasNext() && results.hasNext()) {
                            final AggregatorFactory factory = aggsIter.next();
                            event.put(factory.getName(), factory.deserialize(results.next()));
                        }

                        if (dimsIter.hasNext() || aggsIter.hasNext() || results.hasNext()) {
                            throw new ISE(
                                    "Found left over objects while reading from cache!! dimsIter[%s] aggsIter[%s] results[%s]",
                                    dimsIter.hasNext(),
                                    aggsIter.hasNext(),
                                    results.hasNext()
                            );
                        }

                        return new MapBasedRow(
                                timestamp,
                                event
                        );
                    }
                };
            }

            @Override
            public Sequence<Row> mergeSequences(Sequence<Sequence<Row>> seqOfSequences)
            {
                return new MergeSequence<>(getOrdering(), seqOfSequences);
            }
        };
    }


    /**
     * This function checks the query for dimensions which can be optimized by applying the dimension extraction
     * as the final step of the query instead of on every event.
     *
     * @param query The query to check for optimizations
     *
     * @return A collection of DimensionsSpec which can be extracted at the last second upon query completion.
     */
    public static Collection<DimensionSpec> extractionsToRewrite(GroupQuery query)
    {
        return Collections2.filter(
                query.getDimensions(), new Predicate<DimensionSpec>()
                {
                    @Override
                    public boolean apply(DimensionSpec input)
                    {
                        return input.getExtractionFn() != null
                                && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(
                                input.getExtractionFn().getExtractionType()
                        );
                    }
                }
        );
    }
}
