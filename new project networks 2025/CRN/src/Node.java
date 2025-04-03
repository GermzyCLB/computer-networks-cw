// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Germaine Taylor
//  230021874
//  germaine.taylor@city.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.*;

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */

    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;




    /*
     * These methods query and change how the network is used.
     */




    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;

    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.

    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;


    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;

    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private byte[] nodeHashId;
    private DatagramSocket socket;
    private Deque<String> relayStack = new ArrayDeque<>();

    //to store address key/values...."N:yzx" -> "IP:port"
    private Map<String, String> addressStore = new HashMap<>();

    //for storing data key/values :D:key-> "value"
    private Map<String, String> dataStore = new HashMap<>();

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb1 = new StringBuilder();
        for (byte b : bytes) {
            sb1.append(String.format("%02x", b));
        }
        return sb1.toString();
    }

    // Format a string as per CRN spec: "<number of spaces> <string> "
    // For example, "N:Example" becomes "0 N:Example " (if there are no spaces).
    private String formatCRNString(String s) {
        int spaceCount = 0;
        for (char c : s.toCharArray()) {
            if (c == ' ') spaceCount++;
        }
        return spaceCount + " " + s + " ";
    }


    private String parseCRNString(String formatted) {
        if (formatted == null || formatted.trim().isEmpty()) return "";
        String[] parts = formatted.trim().split(" ", 2);
        if (parts.length < 2) return "";
        return parts[1].substring(0, parts[1].length() - 1).trim();
    }

    // Generate a random two-character transaction ID.
    private String generateTxID() {
        Random rand = new Random();
        char a, b;
        do {
            a = (char) (33 + rand.nextInt(94)); // ASCII 33 to 126
        } while (a == ' ');
        do {
            b = (char) (33 + rand.nextInt(94));
        } while (b == ' ');
        return "" + a + b;
    }

    // Compute the XOR distance between two hash IDs.
    private BigInteger distance(byte[] hash1, byte[] hash2) {
        BigInteger a = new BigInteger(1, hash1);
        BigInteger b = new BigInteger(1, hash2);
        return a.xor(b);
    }

    // Calculate the distance as the number of leading bits that differ
    private int bitDistance(byte[] hash1, byte[] hash2) {
        BigInteger dist = distance(hash1, hash2);
        return 256 - dist.bitLength();
    }


    // Send a request and wait for a response, retrying up to maxRetries if no response is received.

    // Find the three nearest nodes to a given hashID
    // Find the three nodes closest to a given key's hashID
    // Find the three nodes closest to a given key's hashID
    private List<Map.Entry<String, String>> findNearestNodes(String key) throws Exception {
        byte[] targetHash = HashID.computeHashID(key);
        List<Map.Entry<String, String>> nodeList = new ArrayList<>(addressStore.entrySet());
        nodeList.sort((a, b) -> {
            try {
                byte[] hashA = HashID.computeHashID(a.getKey());
                byte[] hashB = HashID.computeHashID(b.getKey());
                return distance(hashA, targetHash).compareTo(distance(hashB, targetHash));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return nodeList.subList(0, Math.min(3, nodeList.size()));
    }

    // Check if this node is among the three closest to a key
    // Check if this node is among the three closest to a key
    private boolean isAmongClosest(String key) throws Exception {
        List<Map.Entry<String, String>> nearest = findNearestNodes(key);
        for (Map.Entry<String, String> node : nearest) {
            if (node.getKey().equals(nodeName)) return true;
        }
        return false;
    }

    // Send a request and wait for a response, with retransmission
    private String sendRequestWithRetransmission(String request, InetAddress address, int port, int timeout, int maxRetries) throws Exception {
        byte[] reqBytes = request.getBytes(StandardCharsets.UTF_8);
        DatagramPacket reqPacket = new DatagramPacket(reqBytes, reqBytes.length, address, port);
        byte[] respBuf = new byte[1024];
        DatagramPacket respPacket = new DatagramPacket(respBuf, respBuf.length);
        int attempts = 0;
        while (attempts < maxRetries) {
            socket.send(reqPacket);
            try {
                socket.setSoTimeout(timeout);
                socket.receive(respPacket);
                return new String(respPacket.getData(), 0, respPacket.getLength(), StandardCharsets.UTF_8);
            } catch (SocketTimeoutException e) {
                attempts++;
            }
        }
        return null;
    }

    // Send a message, using relays if the relayStack is non-empty
    private String sendMessage(String message, String targetNodeName, boolean expectResponse) throws Exception {
        if (relayStack.isEmpty()) {
            String addrString = addressStore.get(targetNodeName);
            if (addrString == null) return null;
            String[] addrParts = addrString.split(":");
            if (addrParts.length != 2) return null;
            InetAddress targetAddress = InetAddress.getByName(addrParts[0]);
            int targetPort = Integer.parseInt(addrParts[1]);
            if (expectResponse) {
                return sendRequestWithRetransmission(message, targetAddress, targetPort, 5000, 3);
            } else {
                byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(msgBytes, msgBytes.length, targetAddress, targetPort);
                socket.send(packet);
                return null;
            }
        } else {
            String relayNode = relayStack.peek();
            String addrString = addressStore.get(relayNode);
            if (addrString == null) return null;
            String[] addrParts = addrString.split(":");
            if (addrParts.length != 2) return null;
            InetAddress relayAddress = InetAddress.getByName(addrParts[0]);
            int relayPort = Integer.parseInt(addrParts[1]);
            String txID = message.substring(0, 2);
            String relayMessage = txID + " V " + formatCRNString(targetNodeName) + message;
            if (expectResponse) {
                return sendRequestWithRetransmission(relayMessage, relayAddress, relayPort, 5000, 3);
            } else {
                byte[] msgBytes = relayMessage.getBytes(StandardCharsets.UTF_8);
                DatagramPacket packet = new DatagramPacket(msgBytes, msgBytes.length, relayAddress, relayPort);
                socket.send(packet);
                return null;
            }
        }

    }

    private void sendingResponse(String response, InetAddress address, int port) throws Exception {
        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length, address, port);
        socket.send(respPacket);
        //System.out.println("Sent response: " + response);

    }


    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("name of node is invalid");
        }
        this.nodeName = nodeName;
        this.nodeHashId = HashID.computeHashID(nodeName);

        // Initialize addressStore with a placeholder IP and port
        addressStore.put(nodeName, "127.0.0.1:0");
    }


    public void openPort(int portNumber) throws Exception {
        if (portNumber < 20110 || portNumber > 20130) {
            throw new Exception("Port number must be between 20110 and 20130");
        }
        socket = new DatagramSocket(portNumber);
        // Update our address with the correct port
        // Update our address with the correct port
        String ip = addressStore.get(nodeName).split(":")[0];
        addressStore.put(nodeName, ip + ":" + portNumber);

    }

    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(delay > 0 ? delay : 10000); // Default to 10 seconds if delay is 0
        try {
            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                String[] parts = message.split(" ", 3);
                if (parts.length < 2) continue;
                String txID = parts[0];
                String type = parts[1];
                String payload = parts.length > 2 ? parts[2] : "";
                switch (type) {
                    case "G":
                        handleNameRequest(txID, packet);
                        break;
                    case "N":
                        handleNearestRequest(txID, packet, payload);
                        break;
                    case "E":
                        handleKeyExistenceRequest(txID, packet, payload);
                        break;
                    case "R":
                        handleReadRequest(txID, packet, payload);
                        break;
                    case "W":
                        handleWriteRequest(txID, packet, payload);
                        break;
                    case "C":
                        handleCASRequest(txID, packet, payload);
                        break;
                    case "V":
                        handleRelayRequest(txID, packet, payload);
                        break;
                    case "I":
                        break; // Ignore information messages
                    default: // Handle unknown messages gracefully
                        System.out.println("Received unknown message type: " + type);
                }
            }
        } catch (SocketTimeoutException e) {
            // Timeout is expected when no messages are received

    }




    }
//this part purely deals with request handlers//

    //from g to h:name request

    private void handleNameRequest(String txID, DatagramPacket requestPacket) throws Exception {
        //this method respons with <txID H <this.nodeName
        String response = txID + " H " + formatCRNString(this.nodeName);
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());

    }

    //from n to o...nearest request
    private void handleNearestRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String hashID = parseCRNString(payload);
        byte[] targetHash;
        try {
            targetHash = HashID.computeHashID(hashID);
        } catch (Exception e) {
            // Malformed hashID, respond with empty list
            String response = txID + " O ";
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            return;
        }
        List<Map.Entry<String, String>> nearest = findNearestNodes(hashID);
        StringBuilder response = new StringBuilder(txID + " O ");
        for (Map.Entry<String, String> node : nearest) {
            response.append(formatCRNString(node.getKey())).append(formatCRNString(node.getValue()));
        }
        sendingResponse(response.toString(), requestPacket.getAddress(), requestPacket.getPort());
    }



    //from e to f: key existence
    private void handleKeyExistenceRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String key = parseCRNString(payload);
        boolean conditionA = dataStore.containsKey(key) || addressStore.containsKey(key);
        boolean conditionB = isAmongClosest(key);
        String responseChar;
        if (conditionA) {
            responseChar = "Y";
        } else if (conditionB) {
            responseChar = "N";
        } else {
            responseChar = "?";
        }
        String response = txID + " F " + responseChar;
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    //from r to s :read
    private void handleReadRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String key = parseCRNString(payload);
        boolean conditionA = dataStore.containsKey(key);
        boolean conditionB = isAmongClosest(key);
        String responseChar;
        String responseValue;
        if (conditionA) {
            responseChar = "Y";
            responseValue = formatCRNString(dataStore.get(key));
        } else if (conditionB) {
            responseChar = "N";
            responseValue = formatCRNString("");
        } else {
            responseChar = "?";
            responseValue = formatCRNString("");
        }
        String response = txID + " S " + responseChar + " " + responseValue;
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    //from w to x :write
    private void handleWriteRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String[] parts = payload.split(" ", 3);
        if (parts.length < 3) {
            String response = txID + " X X";
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            return;
        }
        String keyPart = parts[0] + " " + parts[1] + " ";
        String valuePart = parts[2];
        String key = parseCRNString(keyPart);
        String value = parseCRNString(valuePart);
        System.out.println("[" + nodeName + "] Handling write request for key: " + key + ", value: " + value);
        if (key.startsWith("N:")) {
            boolean exists = addressStore.containsKey(key);
            addressStore.put(key, value);
            String response = txID + " X " + (exists ? "R" : "A");
            System.out.println("[" + nodeName + "] Responding to address write: " + response);
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
        } else if (key.startsWith("D:")) {
            boolean conditionA = dataStore.containsKey(key);
            dataStore.put(key, value);
            String response = txID + " X " + (conditionA ? "R" : "A");
            System.out.println("[" + nodeName + "] Responding to data write: " + response);
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            if (!isAmongClosest(key)) {
                System.out.println("[" + nodeName + "] Not among closest for " + key + ", forwarding");
                dataStore.remove(key);
                List<Map.Entry<String, String>> nearest = findNearestNodes(key);
                for (Map.Entry<String, String> node : nearest) {
                    if (!node.getKey().equals(this.nodeName)) {
                        String nodeName = node.getKey();
                        String newTxID = generateTxID();
                        String request = newTxID + " W " + formatCRNString(key) + formatCRNString(value);
                        sendMessage(request, nodeName, false);
                    }
                }
            }
        }

    }


    //handles compare and response request from : c to d
    private void handleCASRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String[] parts = payload.split(" ", 5);
        if (parts.length < 5) {
            String response = txID + " D X";
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            return;
        }
        String keyPart = parts[0] + " " + parts[1] + " ";
        String currentValuePart = parts[2] + " " + parts[3] + " ";
        String newValuePart = parts[4];
        String key = parseCRNString(keyPart);
        String currentValue = parseCRNString(currentValuePart);
        String newValue = parseCRNString(newValuePart);
        synchronized (this) {
            boolean conditionA = dataStore.containsKey(key);
            boolean conditionB = isAmongClosest(key);
            String responseChar;
            if (conditionA) {
                String storedValue = dataStore.get(key);
                if (storedValue.equals(currentValue)) {
                    dataStore.put(key, newValue);
                    responseChar = "R";
                } else {
                    responseChar = "N";
                }
            } else if (conditionB) {
                dataStore.put(key, newValue);
                responseChar = "A";
            } else {
                responseChar = "X";
            }
            String response = txID + " D " + responseChar;
            sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            if (conditionB && !isAmongClosest(key)) {
                dataStore.remove(key);
                List<Map.Entry<String, String>> nearest = findNearestNodes(key);
                for (Map.Entry<String, String> node : nearest) {
                    String nodeName = node.getKey();
                    String newTxID = generateTxID();
                    String request = newTxID + " W " + formatCRNString(key) + formatCRNString(newValue);
                    sendMessage(request, nodeName, false);
                }
            }
        }
    }


    //method for handling relay:v
    private void handleRelayRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        String[] parts = payload.split(" ", 3);
        if (parts.length < 3) return;
        String targetNodeName = parseCRNString(parts[0] + " " + parts[1]);
        String nestedMessage = parts[2];
        String addrString = addressStore.get(targetNodeName);
        if (addrString == null) return;
        String[] addrParts = addrString.split(":");
        if (addrParts.length != 2) return;
        InetAddress targetAddress = InetAddress.getByName(addrParts[0]);
        int targetPort = Integer.parseInt(addrParts[1]);
        byte[] msgBytes = nestedMessage.getBytes(StandardCharsets.UTF_8);
        DatagramPacket forwardPacket = new DatagramPacket(msgBytes, msgBytes.length, targetAddress, targetPort);
        socket.send(forwardPacket);
        // If the nested message is a request, wait for a response
        char messageType = nestedMessage.charAt(2);
        if (messageType == 'G' || messageType == 'N' || messageType == 'E' || messageType == 'R' ||
                messageType == 'W' || messageType == 'C') {
            byte[] respBuf = new byte[1024];
            DatagramPacket respPacket = new DatagramPacket(respBuf, respBuf.length);
            socket.setSoTimeout(5000);
            try {
                socket.receive(respPacket);
                String response = new String(respPacket.getData(), 0, respPacket.getLength(), StandardCharsets.UTF_8);
                // Replace the transaction ID with the original txID
                response = txID + response.substring(2);
                sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
            } catch (SocketTimeoutException e) {
                // No response received
            }
        }

    }

    //5.sending response helper method


    //the basic crn methods
    public boolean isActive(String nodeName) throws Exception {
        //return true;
        String addrString = addressStore.get(nodeName);
        if (addrString == null) return false;
        String[] addrParts = addrString.split(":");
        if (addrParts.length != 2) return false;
        InetAddress address = InetAddress.getByName(addrParts[0]);
        int port = Integer.parseInt(addrParts[1]);
        String txID = generateTxID();
        String request = txID + " G";
        String response = sendRequestWithRetransmission(request, address, port, 5000, 3);
        if (response == null) return false;
        String[] parts = response.split(" ", 3);
        return parts.length >= 3 && parts[1].equals("H") && parseCRNString(parts[2]).equals(nodeName);
    }

    public void pushRelay(String nodeName) throws Exception {
        if (!addressStore.containsKey(nodeName)) {
            throw new Exception("Unknown node: " + nodeName);
        }
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    public boolean exists(String key) throws Exception {
        if (dataStore.containsKey(key) || addressStore.containsKey(key)) {
            return true;
        }
        List<Map.Entry<String, String>> nearest = findNearestNodes(key);
        for (Map.Entry<String, String> node : nearest) {
            String nodeName = node.getKey();
            String txID = generateTxID();
            String request = txID + " E " + formatCRNString(key);
            String response = sendMessage(request, nodeName, true);
            if (response != null) {
                String[] parts = response.split(" ", 3);
                if (parts.length >= 3 && parts[1].equals("F")) {
                    String responseChar = parts[2];
                    if (responseChar.equals("Y")) return true;
                    if (responseChar.equals("N")) return false;
                }
            }
        }
        return false;
    }


    public String read(String key) throws Exception {
        if (dataStore.containsKey(key)) {
            return dataStore.get(key);
        }
        handleIncomingMessages(1000);
        List<Map.Entry<String, String>> nearest = findNearestNodes(key);
        if (nearest.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> node : nearest) {
            if (node.getKey().equals(this.nodeName)) continue;
            String nodeName = node.getKey();
            String txID = generateTxID();
            String request = txID + " R " + formatCRNString(key);
            String response = sendMessage(request, nodeName, true);
            if (response != null) {
                String[] parts = response.split(" ", 4);
                if (parts.length >= 4 && parts[1].equals("S")) {
                    String responseChar = parts[2];
                    String value = parseCRNString(parts[3]);
                    if (responseChar.equals("Y")) {
                        dataStore.put(key, value);
                        return value;
                    } else if (responseChar.equals("N")) {
                        continue;
                    }
                }
            }
        }
        if (dataStore.containsKey(key) && !isAmongClosest(key)) {
            String value = dataStore.get(key);
            dataStore.remove(key);
            List<Map.Entry<String, String>> newNearest = findNearestNodes(key);
            for (Map.Entry<String, String> node : newNearest) {
                if (!node.getKey().equals(this.nodeName)) {
                    String nodeName = node.getKey();
                    String newTxID = generateTxID();
                    String request = newTxID + " W " + formatCRNString(key) + formatCRNString(value);
                    sendMessage(request, nodeName, false);
                }
            }
        }
        return null;
    }





    public boolean write(String key, String value) throws Exception {
        System.out.println("[" + nodeName + "] Writing key: " + key);
        System.out.println("[" + nodeName + "] addressStore before handleIncomingMessages: " + addressStore);

        // Retry processing messages to ensure we get bootstrap messages
        int attempts = 0;
        while (addressStore.size() <= 1 && attempts < 5) { // Increase to 5 attempts
            handleIncomingMessages(3000); // Increase timeout to 3000ms
            attempts++;
            System.out.println("[" + nodeName + "] Attempt " + (attempts) + " - addressStore: " + addressStore);
        }

        // Validate addressStore entries by checking if nodes are active
        Iterator<Map.Entry<String, String>> iterator = addressStore.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String nodeName = entry.getKey();
            if (nodeName.equals(this.nodeName)) continue; // Skip self
            String addrString = entry.getValue();
            String[] addrParts = addrString.split(":");
            if (addrParts.length != 2) {
                iterator.remove();
                continue;
            }
            try {
                InetAddress address = InetAddress.getByName(addrParts[0]);
                int port = Integer.parseInt(addrParts[1]);
                String txID = generateTxID();
                String request = txID + " G";
                String response = sendRequestWithRetransmission(request, address, port, 1000, 2);
                if (response == null) {
                    System.out.println("[" + this.nodeName + "] Node " + nodeName + " not active, removing from addressStore");
                    iterator.remove();
                }
            } catch (Exception e) {
                System.out.println("[" + this.nodeName + "] Error checking node " + nodeName + ", removing: " + e.getMessage());
                iterator.remove();
            }
        }
        System.out.println("[" + nodeName + "] addressStore after validation: " + addressStore);

        List<Map.Entry<String, String>> nearest = findNearestNodes(key);
        System.out.println("[" + nodeName + "] Nearest nodes for key " + key + ": " + nearest);

        boolean success = false;
        boolean contactedOtherNode = false;

        // Send write requests to the nearest nodes
        for (Map.Entry<String, String> node : nearest) {
            if (node.getKey().equals(this.nodeName)) {
                System.out.println("[" + nodeName + "] Skipping self: " + node.getKey());
                continue;
            }
            contactedOtherNode = true;
            String nodeName = node.getKey();
            String txID = generateTxID();
            String request = txID + " W " + formatCRNString(key) + formatCRNString(value);
            System.out.println("[" + this.nodeName + "] Sending write request to " + nodeName + ": " + request);
            String response = sendMessage(request, nodeName, true);
            System.out.println("[" + this.nodeName + "] Response from " + nodeName + ": " + response);
            if (response != null) {
                String[] parts = response.split(" ", 3);
                if (parts.length >= 3 && parts[1].equals("X")) {
                    String responseChar = parts[2];
                    System.out.println("[" + this.nodeName + "] Response char: " + responseChar);
                    if (responseChar.equals("R") || responseChar.equals("A")) {
                        success = true;
                    }
                } else {
                    System.out.println("[" + this.nodeName + "] Unexpected response format: " + response);
                }
            } else {
                System.out.println("[" + this.nodeName + "] No response from " + nodeName);
            }
        }

        // If we didn't contact any other node or got no successful responses, store locally
        if (!contactedOtherNode || !success) {
            System.out.println("[" + nodeName + "] No successful write to other nodes, storing locally: " + key);
            if (key.startsWith("N:")) {
                addressStore.put(key, value);
            } else if (key.startsWith("D:")) {
                dataStore.put(key, value);
            }
            success = true;
        }

        System.out.println("[" + this.nodeName + "] Write success for " + key + ": " + success);
        return success;
            }






    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        synchronized (this) {
            List<Map.Entry<String, String>> nearest = findNearestNodes(key);
            boolean success = false;
            for (Map.Entry<String, String> node : nearest) {
                String nodeName = node.getKey();
                String txID = generateTxID();
                String request = txID + " C " + formatCRNString(key) + formatCRNString(currentValue) + formatCRNString(newValue);
                String response = sendMessage(request, nodeName, true);
                if (response != null) {
                    String[] parts = response.split(" ", 3);
                    if (parts.length >= 3 && parts[1].equals("D")) {
                        String responseChar = parts[2];
                        if (responseChar.equals("R") || responseChar.equals("A")) {
                            success = true;
                        }
                    }
                }
            }
            return success;
        }
    }

    public static void main(String[] args) {
        try {
            Node node = new Node();
            node.setNodeName("N:ExampleNode");
            node.openPort(20110);

            // Example: Store some sample lines (e.g., Juliet’s speech) in the local dataStore.
            node.write("D:Juliet-1",  "And I'll no longer be a Capulet.");
            node.write("D:Juliet-2",  "'Tis but thy name that is my enemy");
            node.write("D:Juliet-3",  "Thou art thyself, though not a Montague.");
            node.write("D:Juliet-4",  "What's Montague? it is nor hand, nor foot,");
            node.write("D:Juliet-5",  "Nor arm, nor face, nor any other part");
            node.write("D:Juliet-6",  "Belonging to a man. O, be some other name!");
            node.write("D:Juliet-7",  "What's in a name? that which we call a rose");
            node.write("D:Juliet-8",  "By any other name would smell as sweet;");
            node.write("D:Juliet-9",  "So Romeo would, were he not Romeo call'd,");
            node.write("D:Juliet-10", "Retain that dear perfection which he owes");
            node.write("D:Juliet-11", "Without that title. Romeo, doff thy name,");
            node.write("D:Juliet-12", "And for that name which is no part of thee");
            node.write("D:Juliet-13", "Take all myself.");
            node.write("D:Juliet-14", "...");
            node.write("D:Juliet-15", "...");

            // Read back the stored data.
            for (int i = 1; i <= 15; i++) {
                String key = "D:Juliet-" + i;
                String value = node.read(key);
                System.out.println( value);
            }



            // Start listening for incoming messages with a 10-second timeout
            node.handleIncomingMessages(10000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


