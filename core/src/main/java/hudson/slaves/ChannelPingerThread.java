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
import jenkins.util.SystemProperties;
import org.jenkinsci.remoting.RoleChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Created by awpyv on 8/05/2015.
 */
public class ChannelPingerThread implements Runnable {

    private static final int PING_TIMEOUT_SECONDS_DEFAULT = 4 * 60;
    private static final int PING_INTERVAL_SECONDS_DEFAULT = 5 * 60;

    private static final String TIMEOUT_SECONDS_PROPERTY = ChannelPinger.class.getName() + ".pingTimeoutSeconds";
    private static final String INTERVAL_SECONDS_PROPERTY = ChannelPinger.class.getName() + ".pingIntervalSeconds";

    public static final boolean ENABLED =
            System.getProperty(ChannelPinger.class.getName() + ".class", ChannelPinger.class.getName())
                    .equals(ChannelPingerThread.class.getName());

    /**
     * Timeout for the ping in seconds.
     */
    private int pingTimeoutSeconds;

    /**
     * Interval for the ping in seconds.
     */
    private int pingIntervalSeconds;

    private static transient ChannelPingerThread INSTANCE;

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelPingerThread.class);

    private final DelayQueue<Operation> queue = new DelayQueue<>();

    private ExecutorService executorService = Computer.threadPoolForRemoting;
    private Thread thread;
    private Map<Channel,AtomicBoolean> isInClosed = new ConcurrentHashMap<>();
    private Map<Channel,Computer> computers = new ConcurrentHashMap<>();

    private ChannelPingerThread(int pingIntervalSeconds, int pingTimeoutSeconds) {
        this.pingIntervalSeconds = pingIntervalSeconds;
        this.pingTimeoutSeconds = pingTimeoutSeconds;
    }

    @Initializer
    public static void initialize() {
        if (ENABLED) {
            INSTANCE = new ChannelPingerThread(
                    SystemProperties.getInteger(TIMEOUT_SECONDS_PROPERTY, PING_TIMEOUT_SECONDS_DEFAULT, Level.WARNING),
                    SystemProperties.getInteger(INTERVAL_SECONDS_PROPERTY, PING_INTERVAL_SECONDS_DEFAULT, Level.WARNING)
            );
            INSTANCE.start();
        }
    }

    public void start() {
        thread = new Thread(this, getClass().getSimpleName());
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        if (thread != null) {
            thread.interrupt();
            thread = null;
        }

    }

    @Terminator
    public static void terminate() {
        INSTANCE.thread.interrupt();
    }

    @Extension
    public static ComputerListener getComputerListener() {
        return new ComputerListener() {
            @Override
            public void preOnline(Computer c, Channel channel, FilePath root, TaskListener listener) throws IOException, InterruptedException {
                if (INSTANCE != null) {
                    INSTANCE.preOnline(c, channel);
                }
            }
        };
    }

    private void preOnline(Computer c, Channel channel) throws IOException, InterruptedException {
        channel.call(new SetupRemotePing(pingIntervalSeconds, pingTimeoutSeconds));
        INSTANCE.addChannel(channel);
        computers.put(channel, c);
    }

    @Override
    public void run() {

        try {
            //noinspection InfiniteLoopStatement
            while (true) {
                Operation operation = queue.take();
                operation.run();
            }
        } catch (InterruptedException e) {
            // we need to stop
        }

    }

    private void addChannel(Channel channel) {
        LOGGER.trace("adding ping for {}", channel);
        isInClosed.put(channel, new AtomicBoolean(false));

        channel.addListener(new Channel.Listener() {
            @Override
            public void onClosed(Channel channel, IOException cause) {
                AtomicBoolean c = isInClosed.get(channel);
                if (c != null) {
                    c.set(true);
                }
            }
        });

        queue.offer(new Ping(channel, pingIntervalSeconds * 1000));

    }

    private abstract static class Operation implements Delayed {

        final Channel channel;
        final long startTime;
        final long targetTime;

        Operation(Channel channel, long delay) {
            this.channel = channel;
            this.startTime = System.currentTimeMillis();
            this.targetTime = startTime + delay;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long delay = targetTime - System.currentTimeMillis();
            LOGGER.trace("delay for {} is {}ms", channel, delay);
            return unit.convert(delay, TimeUnit.MILLISECONDS);
        }

        abstract void run() throws InterruptedException;

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(targetTime, ((Operation) o).targetTime);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "channel=" + channel +
                    ", startTime=" + startTime +
                    ", targetTime=" + targetTime +
                    '}';
        }
    }

    private class Ping extends Operation {

        Ping(Channel channel, long time) {
            super(channel, time);
            LOGGER.trace("created ping for {}", channel);
        }

        @Override
        void run() throws InterruptedException {
            LOGGER.trace("sending ping for {}", channel);
            try {
                Future<Void> f = channel.callAsync(new PingCommand());
                queue.offer(new Pong(channel, pingTimeoutSeconds * 1000, f));
            } catch (IOException e) {
                LOGGER.trace("failed to send ping for {}", channel, e);
                executorService.submit(new OnDeadCallable(channel, e));
            }
        }
    }

    private class Pong extends Operation {

        final Future f;

        Pong(Channel channel, long time, Future f) {
            super(channel, time);
            this.f = f;
            LOGGER.trace("created pong for {}", channel);
        }

        @Override
        void run() throws InterruptedException {
            long now = System.currentTimeMillis();

            if (f.isDone()) {
                try {
                    f.get();
                    LOGGER.trace("received pong for {}", channel);
                    queue.offer(new Ping(channel, pingIntervalSeconds * 1000));
                } catch (ExecutionException e) {
                    LOGGER.error("ping failed for {}: {}", channel, e.getMessage());
                    if (!(e.getCause() instanceof RequestAbortedException)) {
                        executorService.submit(new OnDeadCallable(channel, e));
                    }
                }
            } else {
                f.cancel(true);
                LOGGER.warn("no pong received for {}", channel);
                executorService.submit(new OnDeadCallable(channel, new TimeoutException("Ping started at " + startTime + " hasn't completed by " + targetTime)));
            }
        }
    }

    private static final class PingCommand implements Callable<Void, IOException> {
        private static final long serialVersionUID = 1L;

        public Void call() {
            return null;
        }

        @Override
        public void checkRoles(RoleChecker checker) throws SecurityException {
            // this callable is literally no-op, can't get any safer than that
        }
    }

    private class OnDeadCallable implements Runnable {
        private final Channel channel;
        private final Throwable cause;

        OnDeadCallable(Channel channel, Throwable cause) {
            this.channel = channel;
            this.cause = cause;
        }

        @Override
        public void run() {
            boolean inClosed = isInClosed.get(channel).get();
            // Disassociate computer channel before closing it
            Computer computer = computers.get(channel);
            if (computer != null) {
                Exception exception = cause instanceof Exception ? (Exception) cause: new IOException(cause);
                computer.disconnect(new OfflineCause.ChannelTermination(exception));
                computers.remove(channel);
            }
            if (inClosed) {
                LOGGER.trace("Ping failed after the channel "+channel.getName()+" is already partially closed.",cause);
            } else {
                LOGGER.info("Ping failed. Terminating the channel "+channel.getName()+".",cause);
            }
            isInClosed.remove(channel);
        }
    }

    private static class SetupRemotePing implements Callable<Void,RuntimeException> {
        private final int pingIntervalSeconds;
        private final int pingTimeoutSeconds;

        SetupRemotePing(int pingIntervalSeconds, int pingTimeoutSeconds) {
            this.pingIntervalSeconds = pingIntervalSeconds;
            this.pingTimeoutSeconds = pingTimeoutSeconds;
        }


        @Override
        public Void call() throws RuntimeException {
            if (INSTANCE != null) {
                INSTANCE.stop();
            }

            INSTANCE = new ChannelPingerThread(pingIntervalSeconds, pingTimeoutSeconds);
            INSTANCE.start();
            INSTANCE.addChannel(Channel.current());

            // trigger classloading for OnDeadCallable, because when the connection is dead, it cannot be loaded
            INSTANCE.new OnDeadCallable(null, null);

            return null;
        }

        @Override
        public void checkRoles(RoleChecker checker) throws SecurityException {

        }
    }
}
