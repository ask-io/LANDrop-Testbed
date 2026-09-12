import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SubnetChatConsole {

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

    public static class MulticastChatEngine {
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

                var interfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
                for (NetworkInterface ni : interfaces) {
                    if (ni.isUp() && !ni.isLoopback() && ni.supportsMulticast()) {
                        for (InterfaceAddress addr : ni.getInterfaceAddresses()) {
                            InetAddress inetAddr = addr.getAddress();
                            if (inetAddr instanceof Inet4Address && inetAddr.getHostAddress().startsWith("192.168.")) {
                                networkInterface = ni;
                                break;
                            }
                        }
                    }
                    if (networkInterface != null) break;
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

    public static void main(String[] args) {
        final String LOCAL_DEVICE_NAME = (args.length > 0) ? args[0] : "Local-Console";

        MulticastChatEngine engine = new MulticastChatEngine();

        // Loopback filtering: drops own echo
        engine.setChatMessageListener(msg -> {
            if (msg.startsWith(LOCAL_DEVICE_NAME + ":")) {
                return;
            }
            System.out.println("\n[Incoming] " + msg);
            System.out.print("> ");
        });

        System.out.println("=================================================");
        System.out.println("         LANDrop Local Subnet Chat Room          ");
        System.out.println("=================================================");
        System.out.println("Joined as: " + LOCAL_DEVICE_NAME);
        System.out.println("Commands:");
        System.out.println("  /peers   - Show currently active devices");
        System.out.println("  /stealth - Toggle stealth mode on/off");
        System.out.println("  /exit    - Leave chat and shutdown");
        System.out.println("-------------------------------------------------");

        engine.start(LOCAL_DEVICE_NAME);

        Scanner scanner = new Scanner(System.in);
        System.out.print("> ");

        while (true) {
            String input = scanner.nextLine().trim();

            if (input.isEmpty()) {
                System.out.print("> ");
                continue;
            }

            if (input.equalsIgnoreCase("/exit")) {
                break;
            } else if (input.equalsIgnoreCase("/peers")) {
                System.out.println("\n--- Active Peers ---");
                if (engine.getActivePeers().isEmpty()) {
                    System.out.println("No peers discovered yet.");
                } else {
                    engine.getActivePeers().forEach((ip, peer) -> {
                        long age = (System.currentTimeMillis() - peer.getLastSeenTimestamp()) / 1000;
                        System.out.printf("  * %s (%s) - seen %ds ago%n", peer.getHostname(), ip, age);
                    });
                }
                System.out.println("--------------------");
            } else if (input.equalsIgnoreCase("/stealth")) {
                boolean nextState = !engine.isStealthMode();
                engine.setStealthMode(nextState);
                System.out.println("Stealth mode is now: " + (nextState ? "ENABLED" : "DISABLED"));
            } else {
                engine.sendChat(LOCAL_DEVICE_NAME, input);
            }

            System.out.print("> ");
        }

        System.out.println("\nShutting down chat service...");
        engine.stop();
        scanner.close();
        System.out.println("Disconnected.");
    }
}