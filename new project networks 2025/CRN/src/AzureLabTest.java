import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;

class AzureLabTest {
    public static void main (String [] args) {
        String emailAddress = "germaine.taylor@city.ac.uk";
        if (false && emailAddress.indexOf('@') == -1) {
            System.err.println("Please set your e-mail address!");
            System.exit(1);
        }
        String ipAddress = "10.216.35.255 ";
        if (false && ipAddress.indexOf('.') == -1) {
            System.err.println("Please set your ip address!");
            System.exit(1);
        }

        try {
            // Create a node and initialise it
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);

            int port = 20110;
            node.openPort(port);

            // Wait and hope that we get sent the address of some other nodes
            System.out.println("Waiting for another node to get in contact");
            node.handleIncomingMessages(12 * 1000);

            // Let's start with a test of reading key/value pairs stored on the network.
            // This should print out a poem.
            System.out.println("Getting the poem...");
            for (int i = 0; i < 7; ++i) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.err.println("Can't find poem verse " + i);
                    System.exit(2);
                } else {
                    System.out.println(value);
                }
            }

            // Now let's test writing a key/value pair
            System.out.println("Writing a marker so it's clear my code works");
            {
                String key = "D:" + emailAddress;
                String value = "It works!";
                boolean success = node.write(key, value);

                // Read it back to be sure
                System.out.println(node.read(key));
            }

            // Finally we will let other nodes know where we are
            // so that we can be contacted and can store data for others.
            System.out.println("Letting other nodes know where we are");
            node.write(nodeName, ipAddress + ":" + port);

            System.out.println("Handling incoming connections");
            node.handleIncomingMessages(0);


        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace(System.err);
            return;
        }
    }
}
