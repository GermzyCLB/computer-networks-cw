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

    // Generate a random two-character transaction ID.
    private String generateTxID() {
        Random rand = new Random();
        char a = (char) ('A' + rand.nextInt(26));
        char b = (char) ('A' + rand.nextInt(26));
        return "" + a + b;
    }

    // Compute the XOR distance between two hash IDs.
    private BigInteger distance(byte[] hash1, byte[] hash2) {
        BigInteger a = new BigInteger(1, hash1);
        BigInteger b = new BigInteger(1, hash2);
        return a.xor(b);
    }


    // Send a request and wait for a response, retrying up to maxRetries if no response is received.
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
                String response = new String(respPacket.getData(), 0, respPacket.getLength(), StandardCharsets.UTF_8);
                return response;
            } catch (SocketTimeoutException e) {
                attempts++;
            }
        }
        return null;
    }


    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("name of node is invalid");
        }
        this.nodeName = nodeName;
        this.nodeHashId = HashID.computeHashID(nodeName);
    }


    public void openPort(int portNumber) throws Exception {
        //throw new Exception("Not implemented");
        socket = new DatagramSocket(portNumber);
        // System.out.println("udp socket has been opened on port" + portNumber);
    }

    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        //set socket timeout only if the delay is more than zero
        if (delay > 0) {
            socket.setSoTimeout(delay);
        }

        try {
            while (true) {
                //successive in receiving an incoming udp port throught process
                socket.receive(packet);
                String received = new String((packet.getData()), 0, packet.getLength(), StandardCharsets.UTF_8);

                //prints whats sent and received
                //System.out.println("Received: " + received + " from " + packet.getAddress().getHostAddress() + ":" + packet.getPort());


                //now we need to parse the CRN message
                //what the correct format needs to be: "<txId> ,<type> [payload]"

                String[] parts = received.split(" ", 3);
                if (parts.length < 2) {
                    //System.out.println("ERROR..invalid message format");
                    continue;
                }
                String txID = parts[0]; //refers to the 2-byte transaction id
                String type = parts[1];  //for example letters like "G" "N"  "R"
                String payload = (parts.length > 2) ? parts[2] : " ";

                switch (type) {
                    //case that requests the name
                    case "G":
                        //System.out.println("[debugger] processing 'G' (name request) from"  + packet.getAddress() + ":" + packet.getPort());
                        handleNameRequest(txID, packet);
                        break;

                    //case that deals with the nearest request
                    case "N":
                        //System.out.println("[DEBUG] Processing 'N' (Nearest Request) with payload: " + payload);
                        handleNearestRequest(txID, packet, payload);
                        break;
                    // case that shows the existence of a key
                    case "E":
                        //System.out.println("[DEBUG] Processing 'E' (Existence Request) with payload: " + payload);
                        handleKeyExistenceRequest(txID, packet, payload);
                        break;

                    //case that deals with the read capability
                    case "R":
                        // System.out.println("[DEBUG] Processing 'R' (Read Request) with payload: " + payload);
                        handleReadRequest(txID, packet, payload);
                        break;

                    //case that deals with write capability
                    case "W":
                        // System.out.println("[DEBUG] Processing 'W' (Write Request) with payload: " + payload);
                        handleWriteRequest(txID, packet, payload);
                        break;

                    //case that deals with compare and swap
                    case "C":
                        //System.out.println("[DEBUG] Processing 'C' (CAS Request) with payload: " + payload);
                        handleCASRequest(txID, packet, payload);
                        break;

                    //handles relay functionality/capability
                    case "V":
                        // System.out.println("[DEBUG] Processing 'V' (Relay Request) with payload: " + payload);
                        handleRelayRequest(txID, packet, payload);
                        break;

                    //purely information (no response needed)
                    case "I":
                        //System.out.println("INFO message: " + payload);
                        break;

                    //default:
                    //  System.out.println("ERROR unhandled message type" + type);


                }
            }


        } catch (SocketTimeoutException e) {
            // Timed out after 'delay' ms with no messages
            System.out.println("[INFO]...No messages received within the specified delay.");
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
        //System.out.println("[DEBUG] Entering handleNearestRequest with payload: " + payload);

        // Compute the hash of the payload (the target key)
        byte[] targetHash = HashID.computeHashID(payload);

        // Create a list of known nodes from addressStore (each entry: key = node name, value = "IP:port")
        List<Map.Entry<String, String>> nodeList = new ArrayList<>(addressStore.entrySet());

        // Sort the nodes by XOR distance (ascending order) from the target hash
        nodeList.sort((a, b) ->
                {
                    try {
                        return distance(HashID.computeHashID(a.getKey()), targetHash)
                                .compareTo(distance(HashID.computeHashID(b.getKey()), targetHash));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        StringBuilder sb = new StringBuilder(txID + " O ");
        int limit = Math.min(3, nodeList.size());
        for (int i = 0; i < limit; i++) {
            Map.Entry<String, String> entry = nodeList.get(i);
            sb.append(formatCRNString(entry.getKey())).append(formatCRNString(entry.getValue()));
        }





        sendingResponse(sb.toString(), requestPacket.getAddress(), requestPacket.getPort());
        //debugger=
      //  System.out.println("[DEBUGGER].. exiting handlenearestrequest method");

    }

    //from e to f: key existence
    private void handleKeyExistenceRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        // payload in this instance is the key
        // "F" response includes a single character either being .... 'Y', 'N', or '?'
        boolean weHaveKey = exists(payload);
        // e.g. demonstration, respond 'Y' if we have it, 'N' otherwise.
        String responseChar = weHaveKey ? "Y" : "N";
        String response = txID + " F " + responseChar;
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    //from r to s :read
    private void handleReadRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        // payload is the key to read
        // "S" response: "S <Y|N|?> <valueIfY>"
        String value = dataStore.get(payload);
        //System.out.println("Reading the key: " + payload + " -> " + value);

        //starts building the crn response ...
        String responseChar = (value != null) ? "Y" : "N";
        String responseValue = (value != null) ? formatCRNString(value) : formatCRNString("");
        String response = txID + " S " + responseChar + " " + responseValue;

        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    //from w to x :write
    private void handleWriteRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        // payload format: "<key> <value>"
        // We can split it once more:
        String[] kv = payload.split(" ", 3);
        if (kv.length < 3) {
            // System.out.println("Invalid write request payload.");
            return;
        }
        String key = kv[1];
        String value = kv[2];

        // System.out.println("Writing key: " + key + " -> " + value);

        // Attempt the write
        boolean success = write(key, value);
        // The RFC says the response char is either 'R', 'A', or 'X' depending on the scenario.
        // For demonstration, respond 'R' if we replaced, 'A' if we added, 'X' otherwise.
        // We'll just do 'R' for success, 'X'
        String responseChar = success ? "R" : "X";
        String response = txID + " X " + responseChar;
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }


    //handles compare and response request from : c to d
    private void handleCASRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        // payload format: "<key> <currentValue> <newValue>"
        // , we do a simple split. A robust approach should parse carefully.
        String[] parts = payload.split(" ", 4);
        if (parts.length < 4) {
            //System.out.println("Invalid CAS payload.");
            return;
        }

        String key = parts[1];
        String currentVal = parts[2];
        String newVal = parts[3];

        boolean success = CAS(key, currentVal, newVal);
        // RFC says 'R' if replaced, 'N' if no match, 'A' if newly added, 'X' if not stored, etc.
        //  'R' = success, 'N'= fail here.
        String responseChar = success ? "R" : "N";
        String response = txID + " D " + responseChar;
        sendingResponse(response, requestPacket.getAddress(), requestPacket.getPort());
    }

    //method for handling relay:v
    private void handleRelayRequest(String txID, DatagramPacket requestPacket, String payload) throws Exception {
        // The payload = "<targetNodeName> <nested CRN message>"
        // e.g: "N:Bob AB G "
        String[] relayParts = payload.split(" ", 2);
        if (relayParts.length < 2) {
            //System.out.println("Invalid relay message: missing nested message");
            return;
        }

        String targetNodeName = relayParts[0];
        String nestedMessage = relayParts[1];

        // finds the address for nodes IP:port in address store
        String addrString = addressStore.get(targetNodeName);
        if (addrString == null) {
            System.out.println("No address known for " + targetNodeName);
            return;
        }
        String[] addrParts = addrString.split(":");
        if (addrParts.length != 2) {
            System.out.println("Invalid address format for " + targetNodeName);
            return;
        }
        String host = addrParts[0];
        int port = Integer.parseInt(addrParts[1]);

        // 2. Forwards the nested message to its respected target node

        byte[] forwardingBytes = nestedMessage.getBytes(StandardCharsets.UTF_8);
        DatagramPacket forwardPacket = new DatagramPacket(forwardingBytes, forwardingBytes.length,
                InetAddress.getByName(host), port);
        socket.send(forwardPacket);
        //System.out.println("Forwarded relay to " + targetNodeName + " -> " + nestedMessage);

        // 3. Wait for a response from the target node (if it’s a request).
        // Then forward that response back to the original sender with the original txID.
        // This is a simplified approach, waiting for exactly one response:
        byte[] responseBuf = new byte[1024];
        DatagramPacket responsePacket = new DatagramPacket(responseBuf, responseBuf.length);
        socket.setSoTimeout(3000); // e.g., 3s wait
        try {
            socket.receive(responsePacket);
            String targetResponse = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
            // Replace the first 2 characters of the targetResponse with txID to preserve the original transaction ID
            if (targetResponse.length() >= 2) {
                targetResponse = txID + targetResponse.substring(2);
            }
            // Send it back to the original sender
            byte[] finalBytes = targetResponse.getBytes(StandardCharsets.UTF_8);
            DatagramPacket finalPacket = new DatagramPacket(
                    finalBytes, finalBytes.length,
                    requestPacket.getAddress(), requestPacket.getPort()
            );
            socket.send(finalPacket);
            // System.out.println("Relayed response back to original sender: " + targetResponse);
        } catch (SocketTimeoutException e) {
            //System.out.println("No response from target node for nested message.");
        }

    }

    //5.sending response helper method
    private void sendingResponse(String response, InetAddress address, int port) throws Exception {
        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length, address, port);
        socket.send(respPacket);
        //System.out.println("Sent response: " + response);

    }

    //the basic crn methods
    public boolean isActive(String nodeName) throws Exception {
        return true;
    }

    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    public boolean exists(String key) throws Exception {
        return dataStore.containsKey(key) || addressStore.containsKey(key);
    }

    public String read(String key) throws Exception {
        // If present in local data store, return it.
        if (dataStore.containsKey(key)) {
            return dataStore.get(key);
        }
        // Otherwise, query nearest nodes.
        // For simplicity, iterate over stored addresses.
        for (String addr : addressStore.values()) {
            String[] parts = addr.split(":");
            if (parts.length != 2) continue;

            InetAddress targetAddress = InetAddress.getByName(parts[0]);
            int targetPort = Integer.parseInt(parts[1]);
            String txID = generateTxID();
            String request = txID + " R " + formatCRNString(key);
            // Send request with retransmission: 5000ms timeout, 3 attempts.
            String response = sendRequestWithRetransmission(request, targetAddress, targetPort, 5000, 3);
            if (response != null) {
                // Expecting a response like: "<txID> S Y <formattedValue>"
                String[] tokens = response.split(" ", 4);
                if (tokens.length >= 4 && tokens[1].equals("S") && tokens[2].equals("Y")) {
                    // tokens[3] should be the formatted value.
                    // For simplicity, remove the numeric prefix and trim.
                    String formatted = tokens[3].trim();
                    int firstSpace = formatted.indexOf(" ");
                    String value = (firstSpace != -1) ? formatted.substring(firstSpace + 1, formatted.lastIndexOf(" ")).trim() : formatted;
                    dataStore.put(key, value);
                    return value;
                }
            }
        }
        return null;
    }

    public boolean write(String key, String value) throws Exception {
        //if its an appropriate address key , store in address store
        if (key.startsWith("N:")) {
            addressStore.put(key, value);
            return true; // Could do distance checks, etc.
        } else if (key.startsWith("D:")) {
            dataStore.put(key, value);
            return true;
        }
        return false;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        synchronized (this) {
            // If address = the  key
            if (key.startsWith("N:")) {
                String existing = addressStore.get(key);
                if (existing == null || existing.equals(currentValue)) {
                    addressStore.put(key, newValue);
                    return true;
                }
                return false;
            }
            // If data = the key
            else if (key.startsWith("D:")) {
                String existing = dataStore.get(key);
                if (existing == null || existing.equals(currentValue)) {
                    dataStore.put(key, newValue);
                    return true;
                }
                return false;
            }
            return false;
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


