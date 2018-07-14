import MQ.Producer.ProducerTemplate;

/**
 * Created by Administrator on 2016/12/16.
 */
public class ProduceTest extends ProducerTemplate {

    public ProduceTest(String mqPullSingletonIp) {
        super(mqPullSingletonIp);
    }

    public static void main(String[] arg) {
        new ProduceTest("tcp://192.168.1.108:8000");//测试用例
    }
}
