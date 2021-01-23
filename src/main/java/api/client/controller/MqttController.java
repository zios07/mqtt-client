package api.client.controller;

import api.client.config.Mqtt;
import api.client.model.MqttSubscribeModel;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping(value = "api/mqtt")
public class MqttController {

    @GetMapping("subscribe")
    public List<MqttSubscribeModel> subscribeChannel(@RequestParam(value = "topics") String[] topics,
                                                     @RequestParam(value = "wait_millis") Integer waitMillis) throws InterruptedException, org.eclipse.paho.client.mqttv3.MqttException {
        List<MqttSubscribeModel> messages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        int topicsLength = topics.length;
        IMqttMessageListener[] listeners = new IMqttMessageListener[topicsLength];
        for (int i = 0; i < listeners.length; i++) {
            listeners[i] = (s, mqttMessage) -> {
                MqttSubscribeModel mqttSubscribeModel = new MqttSubscribeModel();
                mqttSubscribeModel.setId(mqttMessage.getId());
                mqttSubscribeModel.setMessage(new String(mqttMessage.getPayload()));
                mqttSubscribeModel.setQos(mqttMessage.getQos());
                messages.add(mqttSubscribeModel);
                countDownLatch.countDown();
            };
        }
        Mqtt.getInstance().subscribeWithResponse(topics, listeners);
        countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);
        return messages;
    }

    @GetMapping("subscribe/all-topics")
    public List<MqttSubscribeModel> subscribeChannel(@RequestParam(value = "wait_millis") Integer waitMillis) throws InterruptedException, org.eclipse.paho.client.mqttv3.MqttException {
        List<MqttSubscribeModel> messages = new ArrayList<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        String allTopics = "#";
        Mqtt.getInstance().subscribeWithResponse(allTopics, (s, mqttMessage) -> {
            MqttSubscribeModel mqttSubscribeModel = new MqttSubscribeModel();
            mqttSubscribeModel.setId(mqttMessage.getId());
            mqttSubscribeModel.setMessage(new String(mqttMessage.getPayload()));
            mqttSubscribeModel.setQos(mqttMessage.getQos());
            messages.add(mqttSubscribeModel);
            System.out.println(mqttMessage);
            countDownLatch.countDown();
        });
        countDownLatch.await(waitMillis, TimeUnit.MILLISECONDS);
        return messages;
    }

}
