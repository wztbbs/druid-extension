package com.github.wztbbs.watcher;

import io.airlift.command.Cli;
import io.airlift.command.Help;
import io.druid.cli.CliCommandCreator;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by wztbbs on 2015/5/21.
 */
public class RealtimeCommand implements CliCommandCreator {

    @Override
    public void addCommands(Cli.CliBuilder builder) {
        System.out.println("add command......");
        StackTraceElement[] stackElements = Thread.currentThread().getStackTrace();

        if(stackElements != null)
        {
            for(int i = 0; i < stackElements.length; i++)
            {
                System.out.print(stackElements[i].getClassName());
                System.out.print(stackElements[i].getLineNumber());
                System.out.println(stackElements[i].getMethodName());
            }
        }

        System.out.println("-----------------------------------");

        boolean needLoader = true;
        try{
            Field field = builder.getClass().getDeclaredField("groups");
            field.setAccessible(true);
            Map<String, Cli.GroupBuilder> groups = (Map<String, Cli.GroupBuilder>)field.get(builder);
            if(groups.get("wztbbs") != null) {
                needLoader = false;
            }
        }catch (Exception e) {
            e.printStackTrace();
            needLoader = true;
        }

        if(needLoader) {
            builder.withGroup("wztbbs")
                    .withDescription("Run one of the Druid wztbbs types.")
                    .withDefaultCommand(Help.class)
                    .withCommands(
                            CliRealtime.class
                    );
        }
    }
}
