package com.lovecws.mumu.spark.streaming.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 自定义的socket接收器
 * @date 2018-02-07 11:58
 */
public class SocketReceiver extends Receiver<String> {

    String host = null;
    int port = -1;

    public SocketReceiver(String host_, int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                receive();
            }
        }).start();
    }

    @Override
    public void onStop() {
    }

    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            restart("Trying to connect again");
        } catch (ConnectException ce) {
            restart("Could not connect", ce);
        } catch (Throwable t) {
            restart("Error receiving data", t);
        }
    }
}
