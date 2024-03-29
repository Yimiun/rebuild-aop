package io.yuan.pulsar.handlers.amqp.utils.ipUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class IpFilter {

    private static final AtomicBoolean enable = new AtomicBoolean(false);

    private static final AtomicBoolean isStart = new AtomicBoolean(false);

    public volatile static List<String> blackList;

    public volatile static List<String> whiteList;

    private final static Map<String, Boolean> expiredCacheMap = new ConcurrentHashMap<>();

    private final static Set<String> ipDefault = new HashSet<>();

    static {
        ipDefault.add("127.0.0.1");
        ipDefault.add("localhost");
    }

    public static void addDefaultIp(String ip) {
        IpFilter.ipDefault.add(ip);
    }

    public static boolean isMatched(String remoteIp) throws NullPointerException {
        if (enable.get()) {
            remoteIp = remoteIp.replace("/", "").trim();
            if (ipDefault.contains(remoteIp)) {
                return true;
            }
            if (!isStart.get()) {
                log.error("IP Metadata initializing failed, remote Ip {}", remoteIp);
                throw new NullPointerException("IP Metadata initializing failed");
            }
            if (expiredCacheMap.containsKey(remoteIp)) {
                return expiredCacheMap.get(remoteIp);
            }
            if (!handleIpList(remoteIp, blackList) && handleIpList(remoteIp, whiteList)){
                expiredCacheMap.put(remoteIp, true);
                return true;
            } else {
                expiredCacheMap.put(remoteIp, false);
                return false;
            }
        } else {
            return true;
        }
    }

    /**
     * true  : ip matched the list
     * false : ip mismatched the list
     * */
    private static boolean handleIpList(String remoteIp, List<String> ipList) {
        if (ipList == null || ipList.isEmpty()){
            return false;
        }
        if (ipList.contains(remoteIp)) {
            return true;
        }
        boolean matched;
        String[] remoteIpSplit = remoteIp.split("\\.");
        for (String ip:ipList) {
            ip = ip.trim();
            if (ip.contains("*")) {
                matched = true;
                String[] single = ip.split("\\.");
                for (int i=0; i<4; i++) {
                    if (!single[i].equals("*") && !single[i].equals(remoteIpSplit[i])) {
                        matched = false;
                        break;
                    }
                }
                if (matched) {
                    return true;
                }
            }
        }
        return false;
    }

    public static void refreshCacheMap() {
        expiredCacheMap.clear();
    }

    public synchronized static void setIpConfig(String ipConfig, int type) {
        try {
            if (type == 1) {
                IpFilter.whiteList = Arrays.asList(ipConfig.split(","));
            } else if (type == 2){
                IpFilter.blackList = Arrays.asList(ipConfig.split(","));
            }
            IpFilter.refreshCacheMap();
            isStart.set(true);
        } catch (Exception e) {
            log.info("Ip Metadata config analysis failed, wrong template, check your config");
        }
    }

    public static void start() {
        isStart.set(true);
    }

    public static void setEnable(boolean value) {
        enable.set(value);
    }

}
