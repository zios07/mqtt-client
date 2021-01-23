package api.client.controller;

import api.client.config.Mqtt;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping(value = "api/mqtt")
public class MqttController {

    private final ObjectMapper mapper = new ObjectMapper();

    @GetMapping("subscribe")
    public List<Map<String, Object>> subscribeChannel(@RequestParam(value = "topics") String[] topics,
                                                      @RequestParam(value = "wait_millis") Integer waitMillis) throws InterruptedException, org.eclipse.paho.client.mqttv3.MqttException {

        List<Map<String, Object>> messages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        int topicsLength = topics.length;
        IMqttMessageListener[] listeners = new IMqttMessageListener[topicsLength];
        for (int i = 0; i < listeners.length; i++) {
            listeners[i] = (s, mqttMessage) -> {
                Map<String, Object> messageObject = mapper.readValue(new String(mqttMessage.getPayload()), Map.class);
                messages.add(messageObject);
                countDownLatch.countDown();
            };
        }
        Mqtt.getInstance().subscribeWithResponse(topics, listeners);
        countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);
        Mqtt.getInstance().unsubscribe(topics);
        return messages;
    }

    @GetMapping("subscribe/all-topics")
    public List<Map<String, Object>> subscribeChannel(@RequestParam(value = "wait_millis") Integer waitMillis) throws InterruptedException, org.eclipse.paho.client.mqttv3.MqttException {
        List<Map<String, Object>> messages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        String allTopics = "worldcongress2017/#";
        Mqtt.getInstance().subscribeWithResponse(allTopics, (s, mqttMessage) -> {
            Map<String, Object> messageObject = mapper.readValue(new String(mqttMessage.getPayload()), Map.class);
            messages.add(messageObject);
            countDownLatch.countDown();
        });
        countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);
        Mqtt.getInstance().unsubscribe(allTopics);
        return messages;
    }

}
