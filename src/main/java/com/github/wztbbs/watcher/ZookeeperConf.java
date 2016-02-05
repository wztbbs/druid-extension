package com.github.wztbbs.watcher;

/**
 * Created by wztbbs on 2015/5/21.
 */
public class ZookeeperConf {

    private String host = "localhost";

    private int port = 2181;

    private String servicePath = "services";

    private String registerRealtimePath = "";

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getServicePath() {
        return servicePath;
    }

    public void setServicePath(String servicePath) {
        this.servicePath = servicePath;
    }

    public String getRegisterRealtimePath() {
        return registerRealtimePath;
    }

    public void setRegisterRealtimePath(String registerRealtimePath) {
        this.registerRealtimePath = registerRealtimePath;
    }
}
