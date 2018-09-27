package com.alibaba.dubbo.rpc.cluster.router.mesh;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.rpc.*;
import com.alibaba.dubbo.rpc.cluster.Router;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qifeng.zqf on 2018/9/21.
 */
public class MeshRouter implements Router {
    private static final Logger logger = LoggerFactory.getLogger(MeshRouter.class);
    private volatile int priority;
    private volatile boolean force;
    private volatile URL url;
    private String hostIP;
    private volatile boolean mode;
    private String meshport;

    public MeshRouter(URL url) {
        logger.info("constructor parsRule: " + url.getHost() + ":" + url.getIp() + ":" + url.getPath());
        parseRule(url);
    }

    private void parseRule(URL url) {
        this.url = url;
        this.priority = url.getParameter(Constants.PRIORITY_KEY, 0);
        this.force = url.getParameter(Constants.FORCE_KEY, false);
        this.meshport = url.getParameter("meshport", "9090");
        String rule = url.getParameterAndDecoded(Constants.RULE_KEY);
        logger.info("rule: " + rule);
        if (rule == null || rule.trim().length() == 0) {
            throw new IllegalArgumentException("Illegal route rule!");
        }
        rule = rule.replace("consumer.", "").replace("provider.", "");
        int i = rule.indexOf("=>");
        this.hostIP = (i < 0 ? null : rule.substring(0, i).trim()).replaceAll("host|=","").trim();
        this.mode = Boolean.valueOf(i < 0 ? rule.trim() : rule.substring(i + 2).trim());
    }

    @Override
    public URL getUrl() {
        return url;
    }

    public static boolean isMatchGlobPattern(String pattern, String value) {
        if ("*".equals(pattern))
            return true;
        if ((pattern == null || pattern.length() == 0)
                && (value == null || value.length() == 0))
            return true;
        if ((pattern == null || pattern.length() == 0)
                || (value == null || value.length() == 0))
            return false;

        int i = pattern.lastIndexOf('*');
        // doesn't find "*"
        if (i == -1) {
            return value.equals(pattern);
        }
        // "*" is at the end
        else if (i == pattern.length() - 1) {
            return value.startsWith(pattern.substring(0, i));
        }
        // "*" is at the beginning
        else if (i == 0) {
            return value.endsWith(pattern.substring(i + 1));
        }
        // "*" is in the middle
        else {
            String prefix = pattern.substring(0, i);
            String suffix = pattern.substring(i + 1);
            return value.startsWith(prefix) && value.endsWith(suffix);
        }
    }

    boolean matchWhen(URL url) {
        logger.info("match when localIP: " + NetUtils.getLocalHost() + ", hostIP: " + hostIP);
        return isMatchGlobPattern(hostIP, NetUtils.getLocalHost());
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        if (invokers == null || invokers.isEmpty()) {
            return invokers;
        }
        logger.info("route url: " + url + ", hostIP: " + hostIP + ", mode: " + mode);
        try {
            if (!matchWhen(url) || !mode) {
                return invokers;
            } else {
                List<Invoker<T>> result = new ArrayList<Invoker<T>>();
                String serviceDomain = url.getPath() + ".rpc";
                String targetIP = null;
                try {
                    targetIP = InetAddress.getByName(serviceDomain).getHostAddress();
                } catch (UnknownHostException e) {
                    logger.error("can't find target service addresses, target mesh service domain: " + serviceDomain, e);
                }
                if (targetIP != null) {
                    URL invokeURL = invokers.get(0).getUrl();
                    logger.info("mesh router url path1: " + invokeURL);
                    invokeURL.setAddress(targetIP + ":" + meshport);
                    logger.info("mesh router url path2: " + invokeURL);
                    result.add(invokers.get(0));
                }
                if (!result.isEmpty()) {
                    return result;
                } else if (force) {
                    logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(Constants.RULE_KEY));
                    return result;
                }
            }
        } catch (Throwable t) {
            logger.error("Failed to execute mesh router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        return invokers;
    }

    @Override
    public int compareTo(Router o) {
        if (o == null || o.getClass() != MeshRouter.class) {
            return 1;
        }
        MeshRouter c = (MeshRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }
}
