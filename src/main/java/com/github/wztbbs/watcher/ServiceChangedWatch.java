package com.github.wztbbs.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by wztbbs on 2015/5/21.
 */
public class ServiceChangedWatch implements Watcher {

    private RealtimeManager realtimeManager;

    public ServiceChangedWatch(RealtimeManager realtimeManager) {
        this.realtimeManager = realtimeManager;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("accept a watchedEvent:" + watchedEvent);
        if(Event.EventType.NodeChildrenChanged == watchedEvent.getType()) {
            System.out.println("get a node children changed event");
            realtimeManager.updateServicesList();
        }
    }
}
