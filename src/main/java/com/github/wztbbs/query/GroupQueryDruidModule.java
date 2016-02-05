package com.github.wztbbs.query;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.guice.DruidBinders;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 * Created by wztbbs on 2015/10/12.
 */
public class GroupQueryDruidModule implements DruidModule {

    public static final String SCHEME = "group";

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new com.fasterxml.jackson.databind.Module() {
                    @Override
                    public String getModuleName() {
                        return "groupQueryModule-" + System.identityHashCode(this);
                    }

                    @Override
                    public Version version() {
                        return Version.unknownVersion();
                    }

                    @Override
                    public void setupModule(SetupContext context) {
                        context.registerSubtypes(GroupQuery.class);
                    }
                }
        );
    }

    @Override
    public void configure(Binder binder) {
        DruidBinders.queryToolChestBinder(binder)
                .addBinding(GroupQuery.class)
                .to(GroupToolChest.class);

        DruidBinders.queryRunnerFactoryBinder(binder)
                .addBinding(GroupQuery.class)
                .to(GroupQueryRunnerFactory.class);
    }
}
