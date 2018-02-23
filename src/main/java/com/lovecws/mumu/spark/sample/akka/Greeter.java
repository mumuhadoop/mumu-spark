package com.lovecws.mumu.spark.sample.akka;

import akka.actor.*;
import akka.remote.transport.ThrottlerTransportAdapter;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class Greeter extends UntypedActor {
    static public Props props(String message, ActorRef printerActor) {
        return Props.create(Greeter.class, () -> new Greeter(message, printerActor));
    }

    static public class WhoToGreet {
        public final String who;

        public WhoToGreet(String who) {
            this.who = who;
        }
    }

    static public class Greet {
        public Greet() {
        }
    }

    private final String message;
    private final ActorRef printerActor;
    private String greeting = "";

    public Greeter(String message, ActorRef printerActor) {
        this.message = message;
        this.printerActor = printerActor;
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        System.out.println(message.toString());
    }

    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("akka");
        ActorRef actorRef = system.actorOf(Props.empty(), "sparkAkkaStreaming");
        for (int i = 0; i < 1000; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            actorRef.tell("hello word love", ActorRef.noSender());
            System.out.println(actorRef.path().toString());
        }
    }
}