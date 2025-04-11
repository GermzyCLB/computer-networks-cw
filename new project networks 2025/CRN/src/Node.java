// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Germaine Taylor
//  230021874
//  germaine.taylor@city.ac.uk

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;

interface NodeInterface {
    void setNodeName(String nodeName) throws Exception;


    void openPort(int portNumber) throws Exception;


    void handleIncomingMessages(int delay) throws Exception;


    boolean isActive(String nodeName) throws Exception;


    void pushRelay(String nodeName) throws Exception;


    void popRelay() throws Exception;


    boolean exists(String key) throws Exception;


    String read(String key) throws Exception;


    boolean write(String key, String value) throws Exception;


    boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

public class Node implements NodeInterface {

    // all the properties for the nodes
    private String nodeIdentifier;
    private DatagramSocket udpSocket;
    private Thread messageListenerThread;
    private final boolean enableDebug = false;

    // important data structures for the properties of the node
    private final Map<String, String> dataStore = new HashMap<>();
    private final Deque<String> relayStack = new ArrayDeque<>();
    private final Map<String, InetSocketAddress> nodeDirectory = new HashMap<>();
    private final Map<String, String> nearestNodesCache = new HashMap<>();
    private final Set<String> processedMessages = new LinkedHashSet<>() {

       //limit of 100 entries to prevent some memory issues from happening
        @Override
        public boolean add(String e) {
            boolean added = super.add(e);
            if (size() > 1000) {
                iterator().next();
                iterator().remove();
            }
            return added;
        }
    };

    //random num generator for the transaction id's
    private final Random randomGenerator = new Random();

    // what resolves the state for handling responses
    private String lastReadValue = null;
    private boolean lastExistsResult = false;

    //it sets the name of the node in which mut begin with N:
    //and throws an illegalargument exception if the name does not begin with n:
    @Override
    public void setNodeName(String name) {
        if (!name.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'.");
        }
        this.nodeIdentifier = name;
    }
     //opens a udp port for this node to listen in for incoming messages
    //and also starts a background listener thread to handle messages that are
    //coming in and coming out
    @Override
    public void openPort(int portNumber) throws Exception {
        udpSocket = new DatagramSocket(portNumber);
        if (enableDebug) {
            System.out.println("UDP socket opened on port " + portNumber);
        }
        startMessageListener();
    }

    //method is incharge of handling udp messages for the specified duratiom
    //also messages are received and processed all the ay until timeout is reached

    @Override
    public void handleIncomingMessages(int timeoutMillis) throws Exception {
        udpSocket.setSoTimeout(100);
        byte[] buffer = new byte[2048];//buffer for incoming messages
        long startTime = System.currentTimeMillis();

        while (timeoutMillis == 0 || (System.currentTimeMillis() - startTime) < timeoutMillis) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                udpSocket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                if (enableDebug) {
                    System.out.println("Received: " + message);
                }
                processNodeMessage(message, packet.getAddress(), packet.getPort());
            } catch (SocketTimeoutException e) {
                // Timeout is expected, continue looping
            }
        }
    }

    //processes crucial incoming crn message and responds accordingly
    //handles various message types like greetings writes reads and relays

    private void processNodeMessage(String message, InetAddress senderAddress, int senderPort) {
        try {
            // Simulate packet loss
            if (randomGenerator.nextDouble() < 0.1) {
                return;
            }

            String[] parts = message.trim().split("\\s+", 3);
            if (parts.length < 2) {
                return;
            }

            String transactionId = parts[0];
            String messageType = parts[1];

            // Ignore duplicate messages
            if (processedMessages.contains(transactionId)) {
                return;
            }
            processedMessages.add(transactionId);

            String payload = parts.length > 2 ? parts[2] : "";

            switch (messageType) {
                case "G":
                    sendResponse(senderAddress, senderPort, transactionId + " H " + encode(nodeIdentifier));
                    break;
                case "H":
                    String nodeName = decode(payload);
                    if (nodeName != null) {
                        nodeDirectory.put(nodeName, new InetSocketAddress(senderAddress, senderPort));
                    }
                    break;
                case "W":
                    String[] keyValue = parseKeyValuePair(payload);
                    if (keyValue != null && keyValue[0] != null && keyValue[1] != null) {
                        dataStore.put(keyValue[0], keyValue[1]);
                        if (keyValue[0].startsWith("N:")) {
                            try {
                                String[] addressParts = keyValue[1].split(":");
                                if (addressParts.length == 2) {
                                    nodeDirectory.put(keyValue[0], new InetSocketAddress(addressParts[0], Integer.parseInt(addressParts[1])));
                                }
                            } catch (Exception e) {
                                // Ignore malformed address
                            }
                        }
                        sendResponse(senderAddress, senderPort, transactionId + " X A");
                    }
                    break;
                case "R":
                    String readKey = decode(payload);
                    if (readKey != null) {
                        String value = dataStore.getOrDefault(readKey, null);
                        String response = value != null ? transactionId + " S Y " + encode(value) : transactionId + " S N ";
                        sendResponse(senderAddress, senderPort, response);
                    }
                    break;
                case "S":
                    String[] responseParts = payload.split("\\s+", 2);
                    if (responseParts.length > 1 && responseParts[0].equals("Y")) {
                        lastReadValue = decode(responseParts[1]);
                    }
                    break;
                case "E":
                    String existsKey = decode(payload);
                    boolean keyExists = existsKey != null && dataStore.containsKey(existsKey);
                    sendResponse(senderAddress, senderPort, transactionId + " F " + (keyExists ? "Y" : "N"));
                    break;
                case "F":
                    if (payload.trim().equals("Y")) {
                        lastExistsResult = true;
                    }
                    break;
                case "N":
                    String hash = payload.trim();
                    List<String> nodeList = new ArrayList<>(nodeDirectory.keySet());
                    nodeList.removeIf(name -> !name.startsWith("N:"));
                    nodeList.sort((n1, n2) -> {
                        try {
                            return calculateDistance(hash, generateHash(n2)) - calculateDistance(hash, generateHash(n1));
                        } catch (Exception e) {
                            return 0;
                        }
                    });

                    StringBuilder nearestResponse = new StringBuilder(transactionId + " O");
                    int maxNodes = Math.min(3, nodeList.size());
                    for (int i = 0; i < maxNodes; i++) {
                        String name = nodeList.get(i);
                        InetSocketAddress address = nodeDirectory.get(name);
                        if (address != null) {
                            String addressString = address.getAddress().getHostAddress() + ":" + address.getPort();
                            nearestResponse.append(" ").append(encode(name)).append(encode(addressString));
                        }
                    }
                    sendResponse(senderAddress, senderPort, nearestResponse.toString());
                    break;
                case "O":
                    nearestNodesCache.put(transactionId, payload);
                    break;
                case "V":
                    String[] relayParts = payload.split("\\s+", 2);
                    if (relayParts.length == 2) {
                        String nextNode = decode(relayParts[0]);
                        String forwardedMessage = relayParts[1];
                        if (nextNode != null) {
                            if (nextNode.equals(nodeIdentifier)) {
                                processNodeMessage(forwardedMessage, senderAddress, senderPort);
                            } else if (nodeDirectory.containsKey(nextNode)) {
                                InetSocketAddress nextAddress = nodeDirectory.get(nextNode);
                                String relayedMessage = "V " + encode(nextNode) + forwardedMessage;
                                sendResponse(nextAddress.getAddress(), nextAddress.getPort(), relayedMessage);
                            }
                        }
                    }
                    break;
                case "I":
                    // Heartbeat message, no action required
                    break;
                default:
                    // Unknown message type, ignore
                    break;
            }
        } catch (Exception e) {
            if (enableDebug) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        }
    }

    //method sends a response message to the specified address and port
    //applies relay wrapping if the relay stack is not empty
    private void sendResponse(InetAddress address, int port, String message) {
        try {
            String finalMessage = message;
            Deque<String> tempStack = new ArrayDeque<>(relayStack);
            while (!tempStack.isEmpty()) {
                finalMessage = "V " + encode(tempStack.removeLast()) + finalMessage;
            }
            byte[] data = finalMessage.getBytes();
            DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
            udpSocket.send(packet);
            if (enableDebug) {
                System.out.println("Sent: " + finalMessage);
            }
        } catch (IOException e) {
            if (enableDebug) {
                System.err.println("Failed to send response: " + e.getMessage());
            }
        }
    }
//encodes a string for crn message to format by prefixing it with the required amount of spaces
    private String encode(String value) {
        if (value == null) return "";
        int spaceCount = value.length() - value.replace(" ", "").length();
        return spaceCount + " " + value + " ";
    }
    //decodes a stringfrom the crn message format by removing the space count prefix.


    private String decode(String encoded) {
        if (encoded == null || encoded.isEmpty()) return null;
        int firstSpace = encoded.indexOf(' ');
        if (firstSpace == -1 || firstSpace + 1 >= encoded.length()) return null;
        return encoded.substring(firstSpace + 1, encoded.length() - 1);
    }

    //parses a key value pair from a crn message payload
    private String[] parseKeyValuePair(String input) {
        String[] parts = input.trim().split("\\s+", 4);
        if (parts.length == 4) {
            return new String[]{parts[1], parts[3]};
        }
        return null;
    }

    @Override
    public boolean isActive(String nodeName) {
        return nodeDirectory.containsKey(nodeName);
    }

    @Override
    public void pushRelay(String nodeName) {
        relayStack.addLast(nodeName);
    }

    @Override
    public void popRelay() {
        if (!relayStack.isEmpty()) {
            relayStack.removeLast();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        return performLookupOnNode(key, true) != null;
    }

    //reads the value associated with a key from the distributed key-value store

    @Override
    public String read(String key) throws Exception {
        return performLookupOnNode(key, false);
    }

    //performs a lookup for a key in the distributed system

    private String performLookupOnNode(String key, boolean isExistsCheck) throws Exception {
        // Check local store first
        if (dataStore.containsKey(key)) {
            return dataStore.get(key);
        }

        String keyHash = generateHash(key);
        Set<String> visitedNodes = new HashSet<>();
        Deque<String> nodesToVisit = new ArrayDeque<>(nodeDirectory.keySet());

        // Bootstrap with a known node if directory is empty
        if (nodeDirectory.isEmpty()) {
            String bootstrapNode = "N:azure";
            nodeDirectory.put(bootstrapNode, new InetSocketAddress("10.200.51.19", 20114));
            nodesToVisit.add(bootstrapNode);
        }

        while (!nodesToVisit.isEmpty()) {
            String currentNode = nodesToVisit.removeFirst();
            if (visitedNodes.contains(currentNode) || !nodeDirectory.containsKey(currentNode)) {
                continue;
            }
            visitedNodes.add(currentNode);

            InetSocketAddress nodeAddress = nodeDirectory.get(currentNode);
            String transactionId = generateTransactionId();

            // Send the appropriate request
            if (isExistsCheck) {
                lastExistsResult = false;
                sendResponse(nodeAddress.getAddress(), nodeAddress.getPort(), transactionId + " E " + encode(key));
            } else {
                lastReadValue = null;
                sendResponse(nodeAddress.getAddress(), nodeAddress.getPort(), transactionId + " R " + encode(key));
            }

            // Wait for a response
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 1000) {
                handleIncomingMessages(100);
                if (isExistsCheck && lastExistsResult) {
                    return "YES";
                } else if (!isExistsCheck && lastReadValue != null) {
                    return lastReadValue;
                }
            }

            // Request nearest nodes
            String nearestTxId = generateTransactionId();
            sendResponse(nodeAddress.getAddress(), nodeAddress.getPort(), nearestTxId + " N " + keyHash);

            // Wait for nearest nodes response
            startTime = System.currentTimeMillis();
            while (!nearestNodesCache.containsKey(nearestTxId) && (System.currentTimeMillis() - startTime) < 1000) {
                handleIncomingMessages(100);
            }

            String nearestResponse = nearestNodesCache.get(nearestTxId);
            if (nearestResponse == null) {
                continue;
            }

            // Parse nearest nodes and add to queue
            String[] responseParts = nearestResponse.trim().split("\\s+");
            for (int i = 0; i + 3 < responseParts.length; i += 4) {
                String nodeName = responseParts[i + 1];
                String addressString = responseParts[i + 3];
                if (nodeName.startsWith("N:") && addressString.contains(":")) {
                    String[] addressParts = addressString.split(":");
                    InetSocketAddress address = new InetSocketAddress(addressParts[0], Integer.parseInt(addressParts[1]));
                    nodeDirectory.put(nodeName, address);
                    if (!visitedNodes.contains(nodeName)) {
                        nodesToVisit.addLast(nodeName);
                    }
                }
            }
        }
        return null;
    }

    //in charge of writing a key-value pair to the local key value store

    @Override
    public boolean write(String key, String value) {
        dataStore.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String oldValue, String newValue) {
        String currentValue = dataStore.get(key);
        if (currentValue == null) {
            dataStore.put(key, newValue);
            return true;
        }
        if (currentValue.equals(oldValue)) {
            dataStore.put(key, newValue);
            return true;
        }
        return false;
    }

    //in charge of generating a sha-256 hash out of the input
    private String generateHash(String input) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(input.getBytes("UTF-8"));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    //in charge of calculating the distance two hashes using the bitwise operation

    private int calculateDistance(String hash1, String hash2) {
        int distance = 0;
        for (int i = 0; i < hash1.length(); i++) {
            int val1 = Integer.parseInt(hash1.substring(i, i + 1), 16);
            int val2 = Integer.parseInt(hash2.substring(i, i + 1), 16);
            int xor = val1 ^ val2;
            for (int bit = 3; bit >= 0; bit--) {
                if ((xor & (1 << bit)) != 0) {
                    return i * 4 + (3 - bit);
                }
            }
        }
        return 256;
    }

    //Generates a unique transaction id for the crn message
    private String generateTransactionId() {
        char c1 = (char) ('A' + randomGenerator.nextInt(26));
        char c2 = (char) ('A' + randomGenerator.nextInt(26));
        return String.valueOf(c1) + c2;
    }

    public Set<String> getKnownNodeNames() {
        return new HashSet<>(nodeDirectory.keySet());
    }

    //it starts a background thread to listen for incoming messages indefinitely
    private void startMessageListener() {
        messageListenerThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    handleIncomingMessages(0);
                }
            } catch (Exception e) {
                if (enableDebug) {
                    System.err.println("Message listener error: " + e.getMessage());
                }
            }
        });
        messageListenerThread.setDaemon(true);
        messageListenerThread.start();
    }

    // Main method for testing purposes
    //sets up a node and listens for incoming messages
    public static void main(String[] args) {
        try {
            Node node = new Node();
            String name = "N:demoNode";
            node.setNodeName(name);
            System.out.println("Node initialized with name: " + name);

            int portNumber = 54321;
            node.openPort(portNumber);
            System.out.println("Listening on port: " + portNumber);

            node.write("sampleKey", "sampleValue");
            System.out.println("Stored sampleKey:sampleValue");

            System.out.println("Starting to listen for messages...");
            node.handleIncomingMessages(0);
        } catch (Exception e) {
            System.err.println("Error during execution: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
