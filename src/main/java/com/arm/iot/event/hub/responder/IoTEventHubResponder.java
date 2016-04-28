/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.arm.iot.event.hub.responder;

import com.arm.connector.bridge.json.JSONGenerator;
import com.arm.connector.bridge.json.JSONGeneratorFactory;
import com.arm.connector.bridge.json.JSONParser;
import com.microsoft.azure.iot.service.sdk.DeliveryAcknowledgement;
import com.microsoft.azure.iot.service.sdk.FeedbackReceiver;
import com.microsoft.azure.iot.service.sdk.IotHubServiceClientProtocol;
import com.microsoft.azure.iot.service.sdk.Message;
import com.microsoft.azure.iot.service.sdk.ServiceClient;
import java.io.IOException;
import com.microsoft.eventhubs.client.Constants;
import com.microsoft.eventhubs.client.EventHubClient;
import com.microsoft.eventhubs.client.EventHubEnqueueTimeFilter;
import com.microsoft.eventhubs.client.EventHubException;
import com.microsoft.eventhubs.client.EventHubMessage;
import com.microsoft.eventhubs.client.EventHubReceiver;
import com.microsoft.eventhubs.client.ConnectionStringBuilder;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This application processes input telemetry from the mbed-ethernet-sample project (monotonic counter resource) and sends a message back to 
 * the device (via IoTEventHub and the mbed Connector bridge) to toggle the LED resource on or off depending on whether the counter resource
 * is even or odd
 */
public class IoTEventHubResponder {

    private static EventHubClient client;
    private static long now = System.currentTimeMillis();
  
    // Configuration/Connection Parameters
    public static final String connectionString = "HostName=DevelopmentHub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=bcr2LvbLWgk68gxhHBAV6+awJgFB7LplCzfKI1duKVI=";
    public static final String deviceId = "cc69e7c5-c24f-43cf-8365-8d23bb01c707";
    public static final String policyName = "iothubowner";
    public static final String policyKey = "bcr2LvbLWgk68gxhHBAV6+awJgFB7LplCzfKI1duKVI=";
    public static final String namespace = "ihsuprodblres008dednamespace";
    public static final String name = "iothub-ehub-developmen-29279-2e150a3f04";
    public static final String uri = "/123/0/4567";
    
    /** Choose iotHubServiceClientProtocol */
    private static final IotHubServiceClientProtocol protocol = IotHubServiceClientProtocol.AMQPS;

    private static ServiceClient serviceClient = null;
    private static FeedbackReceiver feedbackReceiver = null;

    protected static void openServiceClient() throws Exception
    {
        System.out.println("Creating ServiceClient...");
        serviceClient = ServiceClient.createFromConnectionString(connectionString, protocol);

        CompletableFuture<Void> future = serviceClient.openAsync();
        future.get();
        System.out.println("********* Successfully created an ServiceClient.");
    }

    protected static void closeServiceClient() throws ExecutionException, InterruptedException, IOException
    {
        serviceClient.close();

        CompletableFuture<Void> future = serviceClient.closeAsync();
        future.get();
        serviceClient = null;
        System.out.println("********* Successfully closed ServiceClient.");
    }

    protected static void openFeedbackReceiver() throws ExecutionException, InterruptedException
    {
        if (serviceClient != null)
        {
            feedbackReceiver = serviceClient.getFeedbackReceiver(deviceId);
            if (feedbackReceiver != null)
            {
                CompletableFuture<Void> future = feedbackReceiver.openAsync();
                future.get();
                System.out.println("********* Successfully opened FeedbackReceiver...");
            }
        }
    }

    protected static void closeFeedbackReceiver() throws ExecutionException, InterruptedException
    {
        CompletableFuture<Void> future = feedbackReceiver.closeAsync();
        future.get();
        feedbackReceiver = null;
        System.out.println("********* Successfully closed FeedbackReceiver.");
    }
   
    private static class MessageReceiver implements Runnable {

        public volatile boolean stopThread = false;
        private String partitionId;
        
        private JSONGeneratorFactory     m_json_factory = null;
        private JSONGenerator            m_json_generator = null;
        private JSONParser               m_json_parser = null;

        public MessageReceiver(String partitionId) {
            this.partitionId = partitionId;
            
            // JSON Factory
            this.m_json_factory = JSONGeneratorFactory.getInstance();

            // create the JSON Generator
            this.m_json_generator = this.m_json_factory.newJsonGenerator();

            // create the JSON Parser
            this.m_json_parser = this.m_json_factory.newJsonParser();
        }
        
        private Map parseMessage(String json) {
            return this.m_json_parser.parseJson(json);
        }
        
        // send CoAP GET request for temperature
        private void dispatchTemperatureGET() {
            try {
                // GET Temperature
                String commandMessage = "{ \"path\":\"/303/0/5700\", \"ep\":\"cc69e7c5-c24f-43cf-8365-8d23bb01c707\", \"coap_verb\": \"get\" }";

                // CoAP GET requested
                HashMap<String,String> messageProperties = new HashMap<>();
                messageProperties.put("coap_verb", "get");

                // create the message to send to the device
                Message messageToSend = new Message(commandMessage);
                messageToSend.setDeliveryAcknowledgement(DeliveryAcknowledgement.Full);

                // Setting standard properties
                messageToSend.setMessageId(java.util.UUID.randomUUID().toString());
                Date now = new Date();
                messageToSend.setExpiryTimeUtc(new Date(now.getTime() + 60 * 1000));
                messageToSend.setCorrelationId(java.util.UUID.randomUUID().toString());
                messageToSend.setUserId(java.util.UUID.randomUUID().toString());

                // set the message properties
                messageToSend.clearCustomProperties();
                messageToSend.setProperties(messageProperties);

                // send the message and its properties back through IoTEventHub
                CompletableFuture<Void> completableFuture = serviceClient.sendAsync(deviceId, messageToSend);
                completableFuture.get();
            }
            catch (Exception ex) {
                System.out.println("dispatchTemperatureGET: Exception: " + ex.getMessage());
            }
        }
        
        // process a CoAP GET Response
        private void processGetResponse(Map response) {
            System.out.println("GET RESPONSE: " + response);
        }
        
        // process a CoAP Observation
        private void processObservation(Map observation) {
            boolean do_get = false;
            
            // DEBUG We are processing an observation
            System.out.println("Processing an Observation: " + observation);

            // Get the path... if it is the monotonic counter resource, we will process it... 
            String path = (String)observation.get("path");
            if (path != null && path.contentEquals(IoTEventHubResponder.uri) == true) {
                try {
                    // get the monotonic counter value
                    String value = (String)observation.get("value");
                    Integer counter = Integer.parseInt(value) ;

                    // if the counter is even, turn LED ON... otherwise, turn LED OFF.. we can set the CoAP verb in the JSON itself...
                    String commandMessage = null;
                    if (counter%2 == 0) {
                        // DEBUG
                        System.out.println("processObservation: Turning LED ON");

                        // turn ON
                        commandMessage = "{ \"path\":\"/311/0/5850\", \"new_value\":\"1\", \"ep\":\"cc69e7c5-c24f-43cf-8365-8d23bb01c707\", \"coap_verb\": \"put\" }";
                     }
                    else {
                        // DEBUG
                        System.out.println("processObservation: Turning LED OFF");

                        // turn OFF
                        commandMessage = "{ \"path\":\"/311/0/5850\", \"new_value\":\"0\", \"ep\":\"cc69e7c5-c24f-43cf-8365-8d23bb01c707\", \"coap_verb\": \"put\" }";

                        // we will issue a GET on the temperature
                        do_get = true;
                    }

                    // we can also set the CoAP verb as a message property
                    HashMap<String,String> messageProperties = new HashMap<>();
                    messageProperties.put("coap_verb", "put");

                    // DEBUG
                    System.out.println("processObservation: Message Properties: " + messageProperties);
                    System.out.println("processObservation: Sending LED message: " + commandMessage + " to device: " + deviceId);

                    // create the message to send to the device
                    Message messageToSend = new Message(commandMessage);
                    messageToSend.setDeliveryAcknowledgement(DeliveryAcknowledgement.Full);

                    // Setting standard properties
                    messageToSend.setMessageId(java.util.UUID.randomUUID().toString());
                    Date now = new Date();
                    messageToSend.setExpiryTimeUtc(new Date(now.getTime() + 60 * 1000));
                    messageToSend.setCorrelationId(java.util.UUID.randomUUID().toString());
                    messageToSend.setUserId(java.util.UUID.randomUUID().toString());

                    // set the message properties
                    messageToSend.clearCustomProperties();
                    messageToSend.setProperties(messageProperties);

                    // send the message and its properties back through IoTEventHub
                    CompletableFuture<Void> completableFuture = serviceClient.sendAsync(deviceId, messageToSend);
                    completableFuture.get();

                    // DEBUG
                    System.out.println("processObservation: SENT message: " + commandMessage + " to device: " + deviceId);

                    // now do a GET on temperature if requested
                    if (do_get == true) {
                        // DEBUG
                        System.out.println("processObservation: Dispatching CoAP GET of Temperature resource from device: " + deviceId);
                        this.dispatchTemperatureGET();
                    }
                }
                catch (NumberFormatException | UnsupportedEncodingException | InterruptedException | ExecutionException ex) {
                    System.out.println("processObservation: Exception: " + ex.getMessage());
                }
            }
        }
        
        // process an input event
        private void processEvent(String message) {
            // parse the message
            Map parsed = this.parseMessage(message);
            
            // DEBUG
            System.out.println("processEvent: Message: " + parsed);
                        
            String verb = (String)parsed.get("verb");
            if (verb != null && verb.equalsIgnoreCase("get") == true) {
                // DEBUG We are processing a get() reply
                System.out.println("Processing a GET RESPONSE: " + parsed);
                this.processGetResponse(parsed);
            }
            else {
                // DEBUG We are processing an observation
                System.out.println("Processing an OBSERVATION: " + parsed);
                this.processObservation(parsed);
            }
        }

        @Override
        public void run() {
            try {
                EventHubReceiver receiver = client.getConsumerGroup(null).createReceiver(partitionId, new EventHubEnqueueTimeFilter(now), Constants.DefaultAmqpCredits);
                System.out.println("** Created receiver on partition " + partitionId);
                while (!stopThread) {
                    EventHubMessage message = EventHubMessage.parseAmqpMessage(receiver.receive(5000));
                    if (message != null) {
                        // DEBUG
                        System.out.println("Received: " + partitionId + " (" + message.getOffset() + " | "
                                + message.getSequence() + " | " + message.getEnqueuedTimestamp()
                                + ") => " + message.getDataAsString());
                        
                        // process event
                        this.processEvent(message.getDataAsString());
                    }
                }
                receiver.close();
            }
            catch (EventHubException e) {
                System.out.println("Exception: " + e.getMessage());
            }
        }
    }

    // main method()
    public static void main(String[] args) throws IOException, URISyntaxException, Exception {
        try {
            ConnectionStringBuilder csb = new ConnectionStringBuilder(IoTEventHubResponder.policyName, IoTEventHubResponder.policyKey, IoTEventHubResponder.namespace);
            IoTEventHubResponder.client = EventHubClient.create(csb.getConnectionString(), IoTEventHubResponder.name);
        }
        catch (EventHubException e) {
            System.out.println("Exception: " + e.getMessage());
        }
        
        System.out.println("Starting IoTEventHubResponder. Listening for Device: " + IoTEventHubResponder.deviceId + " Resource URI: " + IoTEventHubResponder.uri);

        IoTEventHubResponder.openServiceClient();
        IoTEventHubResponder.openFeedbackReceiver();

        MessageReceiver mr0 = new MessageReceiver("0");
        MessageReceiver mr1 = new MessageReceiver("1");
        Thread t0 = new Thread(mr0);
        Thread t1 = new Thread(mr1);
        t0.start();
        t1.start();

        System.out.println("Press ENTER to exit.");
        System.in.read();
        mr0.stopThread = true;
        mr1.stopThread = true;
        IoTEventHubResponder.client.close();
        
        IoTEventHubResponder.closeFeedbackReceiver();
        IoTEventHubResponder.closeServiceClient();
    }
}
