package com.github.wztbbs.watcher;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import io.druid.cli.QueryJettyServerInitializer;
import io.druid.guice.*;
import io.druid.metadata.MetadataSegmentPublisher;
import io.druid.query.QuerySegmentWalker;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.NoopSegmentPublisher;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.ChatHandlerResource;
import io.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import io.druid.segment.realtime.firehose.ServiceAnnouncingChatHandlerProvider;
import io.druid.server.QueryResource;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Server;

/**
* Created by wztbbs on 2015/5/21.
*/
public class RealtimeModule implements Module {

    @Override
    public void configure(Binder binder)
    {
        try{
            PolyBind.createChoiceWithDefault(
                    binder,
                    "druid.publish.type",
                    Key.get(SegmentPublisher.class),
                    null,
                    "metadata"
            );
            final MapBinder<String, SegmentPublisher> publisherBinder = PolyBind.optionBinder(
                    binder,
                    Key.get(SegmentPublisher.class)
            );
            publisherBinder.addBinding("noop").to(NoopSegmentPublisher.class).in(LazySingleton.class);
            publisherBinder.addBinding("metadata").to(MetadataSegmentPublisher.class).in(LazySingleton.class);

            PolyBind.createChoice(
                    binder,
                    "druid.realtime.chathandler.type",
                    Key.get(ChatHandlerProvider.class),
                    Key.get(NoopChatHandlerProvider.class)
            );
            final MapBinder<String, ChatHandlerProvider> handlerProviderBinder = PolyBind.optionBinder(
                    binder, Key.get(ChatHandlerProvider.class)
            );
            handlerProviderBinder.addBinding("announce")
                    .to(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
            handlerProviderBinder.addBinding("noop")
                    .to(NoopChatHandlerProvider.class).in(LazySingleton.class);

            JsonConfigProvider.bind(binder, "druid.realtime", RealtimeManagerConfig.class);

            binder.bind(FireDepartment.class)
                    .toProvider(FireDepartmentProvider.class)
                    .in(LazySingleton.class);

            binder.bind(QuerySegmentWalker.class).to(RealtimeManager.class).in(ManageLifecycle.class);
            binder.bind(NodeTypeConfig.class).toInstance(new NodeTypeConfig("realtime"));
            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
            Jerseys.addResource(binder, QueryResource.class);
            Jerseys.addResource(binder, ChatHandlerResource.class);
            LifecycleModule.register(binder, QueryResource.class);
            LifecycleModule.register(binder, Server.class);
        }catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.out.println("configure end....");
        }

    }

}
