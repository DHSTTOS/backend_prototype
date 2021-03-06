package com;


import org.bson.Document;

//TODO: Create Parent Class, a record can be A notification or an Alarm
//TODO: calculate moving average
public class PacketRecord {

    private String L2Protocol;
    private String SourceMACAddress;
    private String L4Protocol;
    private String SourceIPAddress;
    private String PacketSummary;
    private String PacketID;
    private String DestinationIPAddress;
    private Timestamp Timestamp;
    private String DestinationPort;
    private String SourcePort;
    private String L3Protocol;
    private String DestinationMACAddress;

    private String offset;

    public void setoffset(String offset) {
        this.offset = offset;
    }

    public String getOffset() {
        return offset;
    }

    //ankush's thing contains an object so this must also be a object
    public class Timestamp {
        //should be stored as UNIX timestamp? right??
        public String $date;

        public Timestamp(String $date) {
            this.$date = $date;
        }

        @Override
        public String toString() {
            return $date;
        }
    }


    public PacketRecord(String l2Protocol, String sourceMACAddress, String l4Protocol, String sourceIPAddress, String packetSummary, String packetID, String destinationIPAddress, Timestamp timestamp, String destinationPort, String sourcePort, String l3Protocol, String destinationMACAddress) {
        L2Protocol = l2Protocol;
        SourceMACAddress = sourceMACAddress;
        L4Protocol = l4Protocol;
        SourceIPAddress = sourceIPAddress;
        PacketSummary = packetSummary;
        PacketID = packetID;
        DestinationIPAddress = destinationIPAddress;
        Timestamp = timestamp;
        DestinationPort = destinationPort;
        SourcePort = sourcePort;
        L3Protocol = l3Protocol;
        DestinationMACAddress = destinationMACAddress;
    }


    public String getL2Protocol() {
        return L2Protocol;
    }

    public void setL2Protocol(String l2Protocol) {
        L2Protocol = l2Protocol;
    }

    public String getSourceMACAddress() {
        return SourceMACAddress;
    }

    public void setSourceMACAddress(String sourceMACAddress) {
        SourceMACAddress = sourceMACAddress;
    }

    public String getL4Protocol() {
        return L4Protocol;
    }

    public void setL4Protocol(String l4Protocol) {
        L4Protocol = l4Protocol;
    }

    public String getSourceIPAddress() {
        return SourceIPAddress;
    }

    public void setSourceIPAddress(String sourceIPAddress) {
        SourceIPAddress = sourceIPAddress;
    }

    public String getPacketSummary() {
        return PacketSummary;
    }

    public void setPacketSummary(String packetSummary) {
        PacketSummary = packetSummary;
    }

    public String getPacketID() {
        return PacketID;
    }

    public void setPacketID(String packetID) {
        PacketID = packetID;
    }

    public String getDestinationIPAddress() {
        return DestinationIPAddress;
    }

    public void setDestinationIPAddress(String destinationIPAddress) {
        DestinationIPAddress = destinationIPAddress;
    }

    public Timestamp getTimestamp() {
        return Timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        Timestamp = timestamp;
    }

    public String getDestinationPort() {
        return DestinationPort;
    }

    public void setDestinationPort(String destinationPort) {
        DestinationPort = destinationPort;
    }

    public String getSourcePort() {
        return SourcePort;
    }

    public void setSourcePort(String sourcePort) {
        SourcePort = sourcePort;
    }

    public String getL3Protocol() {
        return L3Protocol;
    }

    public void setL3Protocol(String l3Protocol) {
        L3Protocol = l3Protocol;
    }

    public String getDestinationMACAddress() {
        return DestinationMACAddress;
    }

    public void setDestinationMACAddress(String destinationMACAddress) {
        DestinationMACAddress = destinationMACAddress;
    }

    public Document getAsDocument() {

        //ID is the Offset in our case
        Document doc = new Document("_id", getOffset())
                .append("L2 ", L2Protocol)
                .append(" SourceMAC: ", SourceMACAddress)
                .append("L4: ", L4Protocol)
                .append("SourceIP: ", SourceIPAddress)
                .append("summary: ", PacketSummary)
                .append("ID: ", PacketID)
                .append("DestIP: ", DestinationIPAddress)
                .append("Tstamp: ", Timestamp.toString())
                .append("DestPort ", DestinationPort)
                .append("SourcePort: ", SourcePort)
                .append("L3: ", L3Protocol)
                .append("DestMac ", DestinationMACAddress);


        return doc;
    }

    @Override
    public String toString() {
        return "L2: " + L2Protocol + " SourceMAC: " +
                SourceMACAddress + "L4: " +
                L4Protocol + "SourceIP: " +
                SourceIPAddress + "summary: " +
                PacketSummary + "ID: " +
                PacketID + "DestIP: " +
                DestinationIPAddress + "Tstamp: " +
                Timestamp.toString() + "DestPort " +
                DestinationPort + "SourcePort: " +
                SourcePort + "L3: " +
                L3Protocol + "DestMac " +
                DestinationMACAddress;
    }
}
