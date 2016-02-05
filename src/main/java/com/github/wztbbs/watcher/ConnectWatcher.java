package com.github.wztbbs.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.concurrent.CountDownLatch;

/**
 * Created by wztbbs on 2015/5/21.
 */
public class ConnectWatcher implements Watcher {

    private CountDownLatch latch;

    public ConnectWatcher(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            latch.countDown();
        }else {
            System.out.println(watchedEvent.getState().getIntValue());
            System.out.println(watchedEvent.getType().getIntValue());
        }
    }
}
