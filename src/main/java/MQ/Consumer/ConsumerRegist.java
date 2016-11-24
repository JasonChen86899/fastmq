package MQ.Consumer;

import org.zeromq.ZMQ;
import zmq.Address;

/**
 * Created by Jason Chen on 2016/11/23.
 */
public class ConsumerRegist extends Thread{
    private ZMQ.Context context;
    private ZMQ.Socket transfer;
    public ConsumerRegist(String tcpAddress){
        this.context = ZMQ.context(1);
        this.transfer = context.socket(ZMQ.REP);
        this.transfer.bind(tcpAddress);
    }

    public void run(){
        while (!Thread.currentThread().isInterrupted()){
            String consumerIp = transfer.recvStr();
            if (check_ip(consumerIp)){
                
            }else {
                break;
            }
        }
    }

    private boolean check_ip(String ip) {
        if (ip.matches("tcp://[1-255]\\.[1-255]\\.[1-255]\\.[1-255]:[0-65535]"))
            return true;
        else
            return false;
    }
}
