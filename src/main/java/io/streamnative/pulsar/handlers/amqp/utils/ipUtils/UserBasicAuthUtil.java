//package io.streamnative.pulsar.handlers.amqp.utils.ipUtils;
//
//import io.streamnative.pulsar.handlers.amqp.metadata.EventManager;
//import io.streamnative.pulsar.handlers.amqp.metadata.UserAuth;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Collectors;
//
///**
// * 判断用户是否合法，
// * 用DigestUtils.sha256Hex(userEntity.getPassword())对密码加密
// * set /amqp/password/$username
// * {"password": "1234", "encryption_password": "", "vhosts": ["vhost01", "vhost02", "ierp"]}
// */
//@Slf4j
//public class UserBasicAuthUtil {
//
//    private final static Map<String, UserAuth> userAuthInfos = new ConcurrentHashMap<>();
//
//    //判断用户合法
//    public static boolean legerUser(String userName, String password) {
//        UserAuth userAuthInfo = getUserAuthInfo(userName);
//        return userAuthInfo.getEncryptionPassword().equals(DigestUtils.sha256Hex(password))
//                || StringUtils.isBlank(password) && StringUtils.isBlank(userAuthInfo.getPassWord());
//    }
//
//    public static boolean legerVhostConnection(String userName, String vhost) {
//        UserAuth userAuthInfo = getUserAuthInfo(userName);
//        return userAuthInfo.getVhosts().contains(vhost);
//    }
//
//
//    public static void putUserAuthInfo(List<UserAuth> userAuthList) {
//        userAuthList.forEach(userAuth -> putUserAuthInfo(userAuth.getZkPath(), userAuth));
//    }
//
//    public static UserAuth getUserAuthInfo(String userName) {
//        String path = EventManager.basicAuthInfoEvent + "/" + userName;
//        return userAuthInfos.get(path);
//    }
//
//    public static void putUserAuthInfo(String path, UserAuth userAuth) {
//        userAuthInfos.put(path, userAuth);
//    }
//
//    public static void removeUnUsedCache(List<String> nodes) {
//        Set<String> updateSet = nodes.stream().map(m -> EventManager.basicAuthInfoEvent + "/" + m).collect(Collectors.toSet());
//        Set<String> all = userAuthInfos.keySet();
//        all.removeAll(updateSet);
//        all.forEach(userAuthInfos::remove);
//        if (log.isDebugEnabled()) {
//            log.debug("new update map{}", userAuthInfos.keySet());
//        }
//    }
//
//}
