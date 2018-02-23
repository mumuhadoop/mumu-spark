package com.lovecws.mumu.spark.sample.akka;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.remote.transport.ThrottlerTransportAdapter;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

public class Printer extends UntypedActor {
    static public Props props() {
        return Props.create(Printer.class, () -> new Printer());
    }

    static public class Greeting {
        public final String message;

        public Greeting(String message) {
            this.message = message;
        }
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        System.out.println(message);
    }
}