package com.github.wztbbs.query;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

/**
 * Created by wztbbs on 2015/10/9.
 */
public class GroupQueryRunnerFactory implements QueryRunnerFactory {

    private Supplier<GroupByQueryConfig> config;

    private final GroupToolChest toolChest;

    @Inject
    public GroupQueryRunnerFactory(Supplier<GroupByQueryConfig> config, GroupToolChest toolChest) {
        this.config = config;
        this.toolChest = toolChest;
    }

    @Override
    public QueryRunner createRunner(Segment segment) {
        return new GroupQueryRunner(config, segment);
    }

    @Override
    public QueryToolChest getToolchest() {
        return this.toolChest;
    }

    @Override
    public QueryRunner mergeRunners(ExecutorService executorService, Iterable iterable) {
        return new GroupParallelQueryRunner<>(iterable, executorService, config);
    }
}
