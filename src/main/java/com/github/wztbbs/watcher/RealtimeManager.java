package com.github.wztbbs.watcher;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.guice.annotations.Processing;
import io.druid.query.*;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.Sink;
import org.apache.zookeeper.ZooKeeper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Created by wztbbs on 2015/5/21.
 */
public class RealtimeManager implements UpdateServiceList, QuerySegmentWalker {

    private static final EmittingLogger log = new EmittingLogger(RealtimeManager.class);

    private ZookeeperConf zookeeperConf;
    private final FireDepartment templateFireDepartment;
    private final QueryRunnerFactoryConglomerate conglomerate;
    private ExecutorService executorService;
    private ZooKeeper zooKeeper;
    private ServiceChangedWatch serviceChangedWatch;
    private List<String> serviceList;


    /**
     * key=data source name,value=FireChiefs of all partition of that data source
     */
    private final Map<String, List<FireChief>> chiefs;


    @Inject
    public RealtimeManager(
            ZookeeperConf zookeeperConf,
            FireDepartment fireDepartment,
            QueryRunnerFactoryConglomerate conglomerate,
            @JacksonInject @Processing ExecutorService executorService
    )
    {
        this.zookeeperConf = zookeeperConf;
        this.templateFireDepartment = fireDepartment;
        this.conglomerate = conglomerate;
        this.executorService = executorService;

        this.chiefs = Maps.newHashMap();
    }


    @LifecycleStart
    public void start() throws Exception
    {
        serviceList = new ArrayList<String>();
        CountDownLatch latch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(zookeeperConf.getHost() + ":" + zookeeperConf.getPort(), 30000, new ConnectWatcher(latch));

        System.out.println("connect zookeeper success......");
        latch.await();

        serviceChangedWatch = new ServiceChangedWatch(this);
        List<String> list = zooKeeper.getChildren(zookeeperConf.getServicePath(), serviceChangedWatch);
        System.out.println("get service list:"+ list);
        for(String service : list) {
            addDataSource(service);
        }
    }

    public void addDataSource(String serviceName) throws Exception{

        serviceList.add(serviceName);

        DataSchema dataSchema = templateFireDepartment.getDataSchema();
        DataSchema newSchema = new DataSchema(serviceName, dataSchema.getParser(), dataSchema.getAggregators(),dataSchema.getGranularitySpec());

        RealtimeIOConfig realtimeIOConfig = templateFireDepartment.getIOConfig();
        EventReceiverFirehoseFactory firehoseFactory = (EventReceiverFirehoseFactory)realtimeIOConfig.getFirehoseFactory();

        Field field = firehoseFactory.getClass().getDeclaredField("chatHandlerProvider");
        field.setAccessible(true);
        Optional<ChatHandlerProvider> object = (Optional<ChatHandlerProvider>)field.get(firehoseFactory);

        EventReceiverFirehoseFactory newFirehoseFactory = new EventReceiverFirehoseFactory(serviceName, firehoseFactory.getBufferSize(), object.get());


        RealtimeIOConfig newRealtimeIOConfig = new RealtimeIOConfig(newFirehoseFactory, realtimeIOConfig.getPlumberSchool());

        FireDepartment fireDepartment = new FireDepartment(newSchema, newRealtimeIOConfig, templateFireDepartment.getTuningConfig());

        final FireChief chief = new FireChief(fireDepartment);
        List<FireChief> chiefs = this.chiefs.get(serviceName);
        if (chiefs == null) {
            chiefs = new ArrayList<FireChief>();
            this.chiefs.put(serviceName, chiefs);
        }
        chiefs.add(chief);

        chief.setName(String.format("chief-%s", serviceName));
        chief.setDaemon(true);
        chief.init();
        chief.start();
    }


    public void deleteDataSource(String service) {
        List<FireChief> list = chiefs.get(service);
        for(FireChief fireChief : list) {
            CloseQuietly.close(fireChief);
        }
    }


    @LifecycleStop
    public void stop()
    {
        for (Iterable<FireChief> chiefs : this.chiefs.values()) {
            for (FireChief chief : chiefs) {
                CloseQuietly.close(chief);
            }
        }
    }

    public FireDepartmentMetrics getMetrics(String datasource)
    {
        List<FireChief> chiefs = this.chiefs.get(datasource);
        if (chiefs == null) {
            return null;
        }
        FireDepartmentMetrics snapshot = null;
        for (FireChief chief : chiefs) {
            if (snapshot == null) {
                snapshot = chief.getMetrics().snapshot();
            } else {
                snapshot.merge(chief.getMetrics());
            }
        }
        return snapshot;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, Iterable<Interval> intervals)
    {
        return getQueryRunnerForSegments(query, null);
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, Iterable<SegmentDescriptor> specs)
    {
        final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
        final List<String> names = query.getDataSource().getNames();
        return new UnionQueryRunner<T>(
                Iterables.transform(
                        names, new Function<String, QueryRunner>() {
                            @Override
                            public QueryRunner<T> apply(String input) {
                                Iterable<FireChief> chiefsOfDataSource = chiefs.get(input);
                                return chiefsOfDataSource == null ? new NoopQueryRunner() : factory.getToolchest().mergeResults(
                                        factory.mergeRunners(
                                                MoreExecutors.sameThreadExecutor(), // Chaining query runners which wait on submitted chain query runners can make executor pools deadlock
                                                Iterables.transform(
                                                        chiefsOfDataSource, new Function<FireChief, QueryRunner<T>>() {
                                                            @Override
                                                            public QueryRunner<T> apply(FireChief input) {
                                                                return input.getQueryRunner(query);
                                                            }
                                                        }
                                                )
                                        )
                                );
                            }
                        }
                ), conglomerate.findFactory(query).getToolchest()
        );
    }

    @Override
    public void updateServicesList(){
        try{
            List<String> cloneServiceList = new ArrayList<String>(serviceList);
            List<String> list = zooKeeper.getChildren(zookeeperConf.getServicePath(), serviceChangedWatch);
            List<String> deleteList = new ArrayList<String>();

            System.out.println("now service list:"+cloneServiceList);
            System.out.println("get new service list:"+list);

            for(String name : cloneServiceList) {
                if(list.contains(name)) {
                    list.remove(name);
                }else {
                    deleteList.add(name);
                }
            }

            System.out.println("need add data source:" + list);
            System.out.println("need delete data source:" + deleteList);
            if(list != null && !list.isEmpty()) {
                for(String name : list) {
                    addDataSource(name);
                }
            }

            if(deleteList != null && !deleteList.isEmpty()) {
                for(String name : deleteList) {
                    deleteDataSource(name);
                }
            }


        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    private class FireChief extends Thread implements Closeable
    {
        private final FireDepartment fireDepartment;
        private final FireDepartmentMetrics metrics;

        private volatile RealtimeTuningConfig config = null;
        private volatile Firehose firehose = null;
        private volatile Plumber plumber = null;
        private volatile boolean normalExit = true;

        public FireChief(
                FireDepartment fireDepartment
        )
        {
            this.fireDepartment = fireDepartment;

            this.metrics = fireDepartment.getMetrics();
        }

        public void init() throws IOException
        {
            config = fireDepartment.getTuningConfig();

            synchronized (this) {
                try {
                    log.info("Calling the FireDepartment and getting a Firehose.");
                    firehose = fireDepartment.connect();
                    log.info("Firehose acquired!");
                    log.info("Someone get us a plumber!");
                    plumber = fireDepartment.findPlumber();
                    log.info("We have our plumber! service:{%s}", ((EventReceiverFirehoseFactory)fireDepartment.getIOConfig().getFirehoseFactory()).getServiceName());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        public FireDepartmentMetrics getMetrics()
        {
            return metrics;
        }

        @Override
        public void run()
        {
            verifyState();

            final Period intermediatePersistPeriod = config.getIntermediatePersistPeriod();

            try {
                plumber.startJob();

                long nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
                while (firehose.hasMore()) {
                    InputRow inputRow = null;
                    try {
                        try {
                            inputRow = firehose.nextRow();
                        }
                        catch (Exception e) {
                            log.debug(e, "thrown away line due to exception, considering unparseable");
                            metrics.incrementUnparseable();
                            continue;
                        }

                        boolean lateEvent = false;
                        boolean indexLimitExceeded = false;
                        try {
                            lateEvent = plumber.add(inputRow) == -1;
                        }
                        catch (IndexSizeExceededException e) {
                            log.info("Index limit exceeded: %s", e.getMessage());
                            indexLimitExceeded = true;
                        }
                        if (indexLimitExceeded || lateEvent) {
                            metrics.incrementThrownAway();
                            log.debug("Throwing away event[%s]", inputRow);

                            if (indexLimitExceeded || System.currentTimeMillis() > nextFlush) {
                                plumber.persist(firehose.commit());
                                nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
                            }

                            continue;
                        }
                        final Sink sink = plumber.getSink(inputRow.getTimestampFromEpoch());
                        if ((sink != null && !sink.canAppendRow()) || System.currentTimeMillis() > nextFlush) {
                            plumber.persist(firehose.commit());
                            nextFlush = new DateTime().plus(intermediatePersistPeriod).getMillis();
                        }
                        metrics.incrementProcessed();
                    }
                    catch (ParseException e) {
                        if (inputRow != null) {
                            log.error(e, "unparseable line: %s", inputRow);
                        }
                        metrics.incrementUnparseable();
                    }
                }
            }
            catch (RuntimeException e) {
                log.makeAlert(
                        e,
                        "RuntimeException aborted realtime processing[%s]",
                        fireDepartment.getDataSchema().getDataSource()
                ).emit();
                normalExit = false;
                throw e;
            }
            catch (Error e) {
                log.makeAlert(e, "Exception aborted realtime processing[%s]", fireDepartment.getDataSchema().getDataSource())
                        .emit();
                normalExit = false;
                throw e;
            }
            finally {
                CloseQuietly.close(firehose);
                if (normalExit) {
                    plumber.finishJob();
                    plumber = null;
                    firehose = null;
                }
            }
        }

        private void verifyState()
        {
            Preconditions.checkNotNull(config, "config is null, init() must be called first.");
            Preconditions.checkNotNull(firehose, "firehose is null, init() must be called first.");
            Preconditions.checkNotNull(plumber, "plumber is null, init() must be called first.");

            log.info("FireChief[%s] state ok.", fireDepartment.getDataSchema().getDataSource());
        }

        public <T> QueryRunner<T> getQueryRunner(Query<T> query)
        {
            QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
            QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

            return new FinalizeResultsQueryRunner<T>(plumber.getQueryRunner(query), toolChest);
        }

        public void close() throws IOException
        {
            synchronized (this) {
                if (firehose != null) {
                    normalExit = false;
                    firehose.close();
                }
            }
        }
    }
}
