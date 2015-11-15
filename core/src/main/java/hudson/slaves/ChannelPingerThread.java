package hudson.slaves;

import hudson.Extension;
import hudson.FilePath;
import hudson.init.Initializer;
import hudson.init.Terminator;
import hudson.model.Computer;
import hudson.model.TaskListener;
import hudson.remoting.Callable;
import hudson.remoting.Channel;
import hudson.remoting.Future;
import hudson.remoting.RequestAbortedException;
import org.jenkinsci.remoting.RoleChecker;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Created by awpyv on 8/05/2015.
 */
public class ChannelPingerThread extends Thread {

    private static transient ChannelPingerThread INSTANCE;

    private static final Logger LOGGER = Logger.getLogger(ChannelPingerThread.class.getName());

    private final PriorityBlockingQueue<Operation> queue = new PriorityBlockingQueue<Operation>();

    private final long timeout;
    private final long interval;
    private ExecutorService executorService;
    private Listener listener;
	
    @Initializer
    public static void initialize() {
        INSTANCE = new ChannelPingerThread(4 * 60 * 1000, 10 * 60 * 1000, Computer.threadPoolForRemoting);
        INSTANCE.start();
    }

    @Terminator
    public static void terminate() {
        INSTANCE.interrupt();
    }

    @Extension
    public static ComputerListener listener() {
        return new ComputerListener() {
            @Override
            public void preOnline(Computer c, Channel channel, FilePath root, TaskListener listener) throws IOException, InterruptedException {
                if (INSTANCE != null) {
                    INSTANCE.addChannel(channel);
                }
            };
        };
    }

    public ChannelPingerThread(long timeout, long interval, ExecutorService executorService) {
        this.timeout = timeout;
        this.interval = interval;
        this.executorService = executorService;
    }

    @Override
    public void run() {

        try {
            while (true) {
                queue.take().run();
            }
        } catch (InterruptedException e) {
            // we need to stop
        }

    }

    public void addChannel(Channel channel) {
        final AtomicBoolean isInClosed = new AtomicBoolean(false);

        channel.addListener(new Channel.Listener() {
            @Override
            public void onClosed(Channel channel, IOException cause) {
                isInClosed.set(true);
            }
        });

        queue.offer(new Ping(channel, System.currentTimeMillis() + interval));
    }

    private abstract static class Operation implements Comparable<Operation> {

        final Channel channel;
        final long time;

        public Operation(Channel channel, long time) {
            this.channel = channel;
            this.time = time;
        }

        abstract void run() throws InterruptedException;

        @Override
        public int compareTo(Operation o) {
            return Long.compare(time, o.time);
        }
    }

    private class Ping extends Operation {

        public Ping(Channel channel, long time) {
            super(channel, time);
        }

        @Override
        void run() throws InterruptedException {
            while (System.currentTimeMillis() < time) {
                Thread.sleep(time - System.currentTimeMillis());
            }
            try {
                Future<Void> f = channel.callAsync(new PingCommand());
                queue.offer(new Pong(channel, System.currentTimeMillis() + timeout, f));
            } catch (IOException e) {
                executorService.submit(new OnDeadCallable(channel, e));
            }
        }


    }

    private  class Pong extends Operation {

        final Future f;

        public Pong(Channel channel, long time, Future f) {
            super(channel, time);
            this.f = f;
        }

        @Override
        void run() throws InterruptedException {
            long start = System.currentTimeMillis();

            long remaining = time - System.currentTimeMillis();

            do {
                try {
                    f.get(Math.max(1,remaining),TimeUnit.MILLISECONDS);

                    queue.offer(new Ping(channel, System.currentTimeMillis() + interval));

                    return;

                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof RequestAbortedException)) {
                        executorService.submit(new OnDeadCallable(channel, e));
                    }
                    return;
                } catch (TimeoutException e) {
                    // get method waits "at most the amount specified in the timeout",
                    // so let's make sure that it really waited enough
                }
                remaining = time - System.currentTimeMillis();
            } while(remaining>0);

            executorService.submit(new OnDeadCallable(channel, new TimeoutException("Ping started at "+start+" hasn't completed by "+System.currentTimeMillis())));
        }
    }

    public interface Listener {
        void onDead(Channel channel, Throwable diagnosis);
    }

    private static final class PingCommand implements Callable<Void, IOException> {
        private static final long serialVersionUID = 1L;

        public Void call() throws IOException {
            return null;
        }

        @Override
        public void checkRoles(RoleChecker checker) throws SecurityException {
            // this callable is literally no-op, can't get any safer than that
        }
    }

    private class OnDeadCallable implements Runnable {
        private final Channel channel;
        private final Throwable diagnosis;

        public OnDeadCallable(Channel channel, Throwable diagnosis) {
            this.channel = channel;
            this.diagnosis = diagnosis;
        }

        @Override
        public void run() {
            listener.onDead(channel, diagnosis);
        }
    }
}
