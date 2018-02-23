package com.lovecws.mumu.spark.sample.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.io.IOException;

public class AkkaQuickstart {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("sparkAkkaStreaming");
        try {
            final ActorRef printerActor =
                    system.actorOf(Printer.props(), "printerActor");
            final ActorRef howdyGreeter =
                    system.actorOf(Greeter.props("Howdy", printerActor), "howdyGreeter");
            final ActorRef helloGreeter =
                    system.actorOf(Greeter.props("Hello", printerActor), "helloGreeter");
            final ActorRef goodDayGreeter =
                    system.actorOf(Greeter.props("Good day", printerActor), "goodDayGreeter");

            howdyGreeter.tell(new Greeter.WhoToGreet("Akka"), ActorRef.noSender());
            howdyGreeter.tell(new Greeter.Greet(), ActorRef.noSender());

            howdyGreeter.tell(new Greeter.WhoToGreet("Lightbend"), ActorRef.noSender());
            howdyGreeter.tell(new Greeter.Greet(), ActorRef.noSender());

            helloGreeter.tell(new Greeter.WhoToGreet("Java"), ActorRef.noSender());
            helloGreeter.tell(new Greeter.Greet(), ActorRef.noSender());

            goodDayGreeter.tell(new Greeter.WhoToGreet("Play"), ActorRef.noSender());
            goodDayGreeter.tell(new Greeter.Greet(), ActorRef.noSender());

            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
        } finally {
            system.terminate();
        }
    }
}