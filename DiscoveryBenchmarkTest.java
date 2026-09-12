import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class DiscoveryBenchmarkTest {

    // --- Peer Data Structure ---
    public static class PeerNode {
        private final String hostname;
        private final String ipAddress;
        private long lastSeenTimestamp;

        public PeerNode(String hostname, String ipAddress, long lastSeenTimestamp) {
            this.hostname = hostname;
            this.ipAddress = ipAddress;
            this.lastSeenTimestamp = lastSeenTimestamp;
        }

        public String getHostname() { return hostname; }
        public String getIpAddress() { return ipAddress; }
        public long getLastSeenTimestamp() { return lastSeenTimestamp; }
        public void setLastSeenTimestamp(long ts) { this.lastSeenTimestamp = ts; }
    }

    // --- Network Engine ---
    public static class MulticastDiscoveryEngine {
        public static final String MULTICAST_GROUP = "230.0.0.1";
        public static final int MULTICAST_PORT = 4446;
        private static final int BUFFER_SIZE = 1024;
        private static final long PEER_TIMEOUT_MS = 10000;

        private static final String PREFIX_DISCOVERY = "LANDROP_DISCOVERY:";
        private static final String PREFIX_CHAT = "LANDROP_CHAT:";

        private final ConcurrentHashMap<String, PeerNode> activePeers = new ConcurrentHashMap<>();
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private final AtomicBoolean isStealth = new AtomicBoolean(false);

        private MulticastSocket socket;
        private InetAddress groupAddress;
        private NetworkInterface networkInterface;

        private Thread beaconThread;
        private Thread listenerThread;
        private Thread cleanupThread;
        private Consumer<String> chatListener;

        public void setChatMessageListener(Consumer<String> listener) { this.chatListener = listener; }
        public void setStealthMode(boolean stealth) { this.isStealth.set(stealth); }
        public boolean isStealthMode() { return this.isStealth.get(); }
        public Map<String, PeerNode> getActivePeers() { return Collections.unmodifiableMap(activePeers); }

        public synchronized void start(String localHostname) {
            if (isRunning.get()) return;

            try {
                groupAddress = InetAddress.getByName(MULTICAST_GROUP);
                socket = new MulticastSocket(MULTICAST_PORT);
                socket.setReuseAddress(true);

                networkInterface = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
                if (networkInterface == null || !networkInterface.isUp() || networkInterface.isLoopback()) {
                    var interfaces = NetworkInterface.getNetworkInterfaces();
                    while (interfaces.hasMoreElements()) {
                        NetworkInterface ni = interfaces.nextElement();
                        if (ni.isUp() && !ni.isLoopback() && ni.supportsMulticast()) {
                            networkInterface = ni;
                            break;
                        }
                    }
                }

                SocketAddress groupSocketAddress = new InetSocketAddress(groupAddress, MULTICAST_PORT);
                socket.joinGroup(groupSocketAddress, networkInterface);
                isRunning.set(true);

                beaconThread = new Thread(() -> {
                    String beacon = PREFIX_DISCOVERY + localHostname;
                    byte[] buf = beacon.getBytes(StandardCharsets.UTF_8);
                    while (isRunning.get()) {
                        try {
                            if (!isStealth.get() && socket != null && !socket.isClosed()) {
                                socket.send(new DatagramPacket(buf, buf.length, groupAddress, MULTICAST_PORT));
                            }
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (IOException ignored) {}
                    }
                }, "BeaconThread");
                beaconThread.setDaemon(true);
                beaconThread.start();

                listenerThread = new Thread(() -> {
                    byte[] buf = new byte[BUFFER_SIZE];
                    while (isRunning.get()) {
                        try {
                            DatagramPacket packet = new DatagramPacket(buf, buf.length);
                            socket.receive(packet);
                            if (isStealth.get()) continue;

                            String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                            String senderIp = packet.getAddress().getHostAddress();

                            if (msg.startsWith(PREFIX_DISCOVERY)) {
                                String host = msg.substring(PREFIX_DISCOVERY.length()).trim();
                                activePeers.compute(senderIp, (ip, node) -> {
                                    if (node == null) return new PeerNode(host, ip, System.currentTimeMillis());
                                    node.setLastSeenTimestamp(System.currentTimeMillis());
                                    return node;
                                });
                            } else if (msg.startsWith(PREFIX_CHAT) && chatListener != null) {
                                chatListener.accept(msg.substring(PREFIX_CHAT.length()).trim());
                            }
                        } catch (SocketException ignored) {
                            break;
                        } catch (IOException ignored) {}
                    }
                }, "ListenerThread");
                listenerThread.setDaemon(true);
                listenerThread.start();

                cleanupThread = new Thread(() -> {
                    while (isRunning.get()) {
                        try {
                            Thread.sleep(3000);
                            long cutoff = System.currentTimeMillis() - PEER_TIMEOUT_MS;
                            activePeers.entrySet().removeIf(e -> e.getValue().getLastSeenTimestamp() < cutoff);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }, "CleanupThread");
                cleanupThread.setDaemon(true);
                cleanupThread.start();

            } catch (IOException e) {
                System.err.println("Init failed: " + e.getMessage());
                stop();
            }
        }

        public void sendChat(String sender, String message) {
            if (!isRunning.get() || socket == null || socket.isClosed()) return;
            try {
                byte[] bytes = (PREFIX_CHAT + sender + ": " + message).getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(bytes, bytes.length, groupAddress, MULTICAST_PORT));
            } catch (IOException ignored) {}
        }

        public synchronized void stop() {
            isRunning.set(false);
            if (socket != null && !socket.isClosed()) {
                try {
                    if (groupAddress != null && networkInterface != null) {
                        socket.leaveGroup(new InetSocketAddress(groupAddress, MULTICAST_PORT), networkInterface);
                    }
                } catch (IOException ignored) {}
                finally {
                    socket.close();
                }
            }
            if (beaconThread != null) beaconThread.interrupt();
            if (listenerThread != null) listenerThread.interrupt();
            if (cleanupThread != null) cleanupThread.interrupt();
            activePeers.clear();
        }
    }

    // --- Benchmark Execution ---
    public static void main(String[] args) {
        final String LOCAL_NODE = (args.length > 0) ? args[0] : "Test-Node";

        System.out.println("=================================================");
        System.out.println("       LANDrop Multicast Discovery Testbed       ");
        System.out.println("=================================================");
        System.out.println("[CONFIG] Local Node Identity : " + LOCAL_NODE);
        System.out.println("[CONFIG] Multicast Group     : 230.0.0.1:4446");
        System.out.println("-------------------------------------------------");

        MulticastDiscoveryEngine engine = new MulticastDiscoveryEngine();
        engine.setChatMessageListener(msg -> System.out.println("\n>>> [CHAT INBOUND] " + msg));

        System.out.println("[STAGE 1/5] Starting discovery threads...");
        engine.start(LOCAL_NODE);
        System.out.println("[OK] Engine running.\n");

        try {
            System.out.println("[STAGE 2/5] Sampling subnet for 3 cycles...");
            for (int poll = 1; poll <= 3; poll++) {
                Thread.sleep(3000);
                System.out.println("\n--- [Peer Snapshot #" + poll + "] ---");
                System.out.println("Total Nodes: " + engine.getActivePeers().size());
                engine.getActivePeers().forEach((ip, peer) -> {
                    long ageSec = (System.currentTimeMillis() - peer.getLastSeenTimestamp()) / 1000;
                    System.out.printf("  * Host: %-15s | IP: %-15s | Seen: %ds ago%n",
                            peer.getHostname(), ip, ageSec);
                });
            }

            System.out.println("\n[STAGE 3/5] Broadcasting chat verification payload...");
            engine.sendChat(LOCAL_NODE, "Probe packet from " + LOCAL_NODE);
            Thread.sleep(1500);

            System.out.println("\n[STAGE 4/5] Toggling stealth mode ON...");
            engine.setStealthMode(true);
            System.out.println("[STATE] Stealth active: " + engine.isStealthMode());
            Thread.sleep(2000);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            System.out.println("\n[STAGE 5/5] Releasing sockets and stopping threads...");
            engine.stop();
            System.out.println("[OK] Testbed teardown completed.");
            System.out.println("=================================================");
        }
    }
}
