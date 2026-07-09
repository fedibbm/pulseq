import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class QueueManager {
    public static final int MAX_CAPACITY = 1000;
    private Map<String, MessageQueue> queues;


    public QueueManager(){
        this.queues = new ConcurrentHashMap<>() ;
    }

    void publish(String topic, Message message){
        if(!this.queues.containsKey(topic)){
            this.createQueue(topic);
        }
        getQueue(topic).enqueue(message);
    }
    public MessageQueue getQueue(String topic){
        return this.queues.get(topic);
    }

    boolean createQueue(String topic){
        if(!this.queues.containsKey(topic)){
            this.queues.put(topic, new MessageQueue(topic, MAX_CAPACITY));
            return true;
        }
        return false;
    }

    List<String> listTopics(){
        return new ArrayList<>(this.queues.keySet());
    }

}
