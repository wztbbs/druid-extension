package com.github.wztbbs.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.NoopLimitSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by wztbbs on 2015/8/25.
 */
@JsonTypeName(GroupQueryDruidModule.SCHEME)
public class GroupQuery extends BaseQuery<Row> {

    private static final Logger logger = LoggerFactory.getLogger(GroupQuery.class);

    private final LimitSpec limitSpec;
    private final HavingSpec havingSpec;
    private final DimFilter dimFilter;
    private final QueryGranularity granularity;
    private final List<DimensionSpec> dimensions;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final List<PostAggregator> postAggregatorSpecs;
    private final boolean containsHashKey;

    private final Function<Sequence<Row>, Sequence<Row>> limitFn;

    @JsonCreator
    public GroupQuery(
            @JsonProperty("dataSource") DataSource dataSource,
            @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
            @JsonProperty("filter") DimFilter dimFilter,
            @JsonProperty("granularity") QueryGranularity granularity,
            @JsonProperty("dimensions") List<DimensionSpec> dimensions,
            @JsonProperty("containsHashKey") boolean containsHashKey,
            @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
            @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
            @JsonProperty("having") HavingSpec havingSpec,
            @JsonProperty("limitSpec") LimitSpec limitSpec,
            @JsonProperty("context") Map<String, Object> context
    )
    {
        super(dataSource, querySegmentSpec, context);
        logger.info("get a group by object, limit spec:{}, having:{}", limitSpec, havingSpec);
        this.dimFilter = dimFilter;
        this.granularity = granularity;
        this.containsHashKey = containsHashKey;
        this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
        this.aggregatorSpecs = aggregatorSpecs;
        this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;
        this.havingSpec = havingSpec;
        this.limitSpec = (limitSpec == null) ?  new NoopLimitSpec() : limitSpec;

        Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
        Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
        Queries.verifyAggregations(this.aggregatorSpecs, this.postAggregatorSpecs);

        Function<Sequence<Row>, Sequence<Row>> postProcFn =
                this.limitSpec.build(this.dimensions, this.aggregatorSpecs, this.postAggregatorSpecs);

        if (havingSpec != null) {
            postProcFn = Functions.compose(
                    new Function<Sequence<Row>, Sequence<Row>>() {
                        @Override
                        public Sequence<Row> apply(Sequence<Row> input) {
                            return Sequences.filter(
                                    input,
                                    new Predicate<Row>() {
                                        int number = 0;
                                        int falseRow = 0;

                                        @Override
                                        public boolean apply(Row input) {
                                            number ++;
                                            boolean  match = GroupQuery.this.havingSpec.eval(input);
                                            if(!match) {
                                                falseRow ++;
                                            }
                                            if(number % 100000 == 0) {
                                                logger.info("number is:{}, false Row is:{}", number, falseRow);
                                            }
                                            return match;
                                        }
                                    }
                            );
                        }
                    },
                    postProcFn
            );
        }

        limitFn = postProcFn;
    }

    /**
     * A private constructor that avoids all of the various state checks.  Used by the with*() methods where the checks
     * have already passed in order for the object to exist.
     */
    private GroupQuery(
            DataSource dataSource,
            QuerySegmentSpec querySegmentSpec,
            DimFilter dimFilter,
            QueryGranularity granularity,
            List<DimensionSpec> dimensions,
            boolean containsHashKey,
            List<AggregatorFactory> aggregatorSpecs,
            List<PostAggregator> postAggregatorSpecs,
            HavingSpec havingSpec,
            LimitSpec orderBySpec,
            Function<Sequence<Row>, Sequence<Row>> limitFn,
            Map<String, Object> context
    )
    {
        super(dataSource, querySegmentSpec, context);

        this.containsHashKey = containsHashKey;
        this.dimFilter = dimFilter;
        this.granularity = granularity;
        this.dimensions = dimensions;
        this.aggregatorSpecs = aggregatorSpecs;
        this.postAggregatorSpecs = postAggregatorSpecs;
        this.havingSpec = havingSpec;
        this.limitSpec = orderBySpec;
        this.limitFn = limitFn;
    }

    @JsonProperty("filter")
    public DimFilter getDimFilter()
    {
        return dimFilter;
    }

    @JsonProperty
    public QueryGranularity getGranularity()
    {
        return granularity;
    }

    @JsonProperty
    public List<DimensionSpec> getDimensions()
    {
        return dimensions;
    }

    @JsonProperty("aggregations")
    public List<AggregatorFactory> getAggregatorSpecs()
    {
        return aggregatorSpecs;
    }

    @JsonProperty("postAggregations")
    public List<PostAggregator> getPostAggregatorSpecs()
    {
        return postAggregatorSpecs;
    }

    @JsonProperty("having")
    public HavingSpec getHavingSpec()
    {
        return havingSpec;
    }

    @JsonProperty
    public LimitSpec getLimitSpec()
    {
        return limitSpec;
    }

    @Override
    public boolean hasFilters()
    {
        return dimFilter != null;
    }

    @Override
    public String getType()
    {
        return GROUP_BY;
    }

    public boolean isContainsHashKey() {
        return containsHashKey;
    }

    public Sequence<Row> applyLimit(Sequence<Row> results)
    {
        return limitFn.apply(results);
    }

    @Override
    public GroupQuery withOverriddenContext(Map<String, Object> contextOverride)
    {
        return new GroupQuery(
                getDataSource(),
                getQuerySegmentSpec(),
                dimFilter,
                granularity,
                dimensions,
                containsHashKey,
                aggregatorSpecs,
                postAggregatorSpecs,
                havingSpec,
                limitSpec,
                limitFn,
                computeOverridenContext(contextOverride)
        );
    }

    @Override
    public GroupQuery withQuerySegmentSpec(QuerySegmentSpec spec)
    {
        return new GroupQuery(
                getDataSource(),
                spec,
                dimFilter,
                granularity,
                dimensions,
                containsHashKey,
                aggregatorSpecs,
                postAggregatorSpecs,
                havingSpec,
                limitSpec,
                limitFn,
                getContext()
        );
    }

    @Override
    public Query<Row> withDataSource(DataSource dataSource)
    {
        return new GroupQuery(
                dataSource,
                getQuerySegmentSpec(),
                dimFilter,
                granularity,
                dimensions,
                containsHashKey,
                aggregatorSpecs,
                postAggregatorSpecs,
                havingSpec,
                limitSpec,
                limitFn,
                getContext()
        );
    }
}
