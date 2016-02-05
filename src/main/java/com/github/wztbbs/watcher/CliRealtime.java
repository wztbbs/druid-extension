package com.github.wztbbs.watcher;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.logger.Logger;
import io.airlift.command.Command;
import io.druid.cli.ServerRunnable;

import java.util.List;

/**
 * Created by wztbbs on 2015/5/21.
 */
@Command(
        name = "realtime",
        description = "Runs a realtime node, see http://druid.io/docs/latest/Realtime.html for a description"
)
public class CliRealtime extends ServerRunnable {

    private static final Logger log = new Logger(CliRealtime.class);

    public CliRealtime()
    {
        super(log);
    }

    @Override
    protected List<? extends Module> getModules()
    {
        return ImmutableList.<Module>of(
                new RealtimeModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder) {
                        binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/realtime");
                        binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8084);
                    }
                }
        );
    }
}
