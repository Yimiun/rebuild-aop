import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class testJson {

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper JSON_MAPPER = new JsonMapper();
        List<String> list = JSON_MAPPER.readValue("[\"q2\",\"q3\",\"q1\"]", new TypeReference<ArrayList<String>>(){});
        list.forEach(System.out::println);
        System.out.println(list);

    }
}
