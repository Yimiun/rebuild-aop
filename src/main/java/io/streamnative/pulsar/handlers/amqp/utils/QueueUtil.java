///**
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.streamnative.pulsar.handlers.amqp.utils;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.json.JsonMapper;
//import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.*;
//
//@Slf4j
//public class QueueUtil {
//
//    public static final ObjectMapper JSON_MAPPER = new JsonMapper();
//
//    public static Map<String, String> generateTopicProperties(String queueName,
//                                                              boolean passive,
//                                                              boolean durable,
//                                                              boolean exclusive,
//                                                              boolean autoDelete,
//                                                              Map<String, Object> arguments) throws JsonProcessingException {
//        if (StringUtils.isEmpty(queueName)) {
//            throw new AoPServiceRuntimeException.QueueParameterException("Miss parameter queue name.");
//        }
//        Map<String, String> props = new HashMap<>();
//        props.put(QUEUE, queueName);
//        props.put(PASSIVE, "" + passive);
//        props.put(DURABLE, "" + durable);
//        props.put(EXCLUSIVE, "" + exclusive);
//        props.put(AUTO_DELETE, "" + autoDelete);
//        if (arguments != null && !arguments.isEmpty()) {
//            props.put(ARGUMENTS, covertObjectValueAsString(arguments));
//        }
//        return props;
//    }
//
//    public static String covertObjectValueAsString(Object obj) throws JsonProcessingException {
//        return JSON_MAPPER.writeValueAsString(obj);
//    }
//
//}
