package com.github.wztbbs.watcher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.guice.RealtimeManagerConfig;
import io.druid.segment.realtime.FireDepartment;

import java.util.List;

/**
 * Created by wztbbs on 2015/5/25.
 */
public class FireDepartmentProvider implements Provider<FireDepartment> {

    private final FireDepartment fireDepartment;


    @Inject
    public FireDepartmentProvider(
            ObjectMapper jsonMapper,
            RealtimeManagerConfig config
    )
    {
        try {
            this.fireDepartment = jsonMapper.readValue(
                    config.getSpecFile(), new TypeReference<FireDepartment>()
                    {
                    }
            );
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public FireDepartment get() {
        return fireDepartment;
    }
}
