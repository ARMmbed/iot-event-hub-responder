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
import org.bouncycastle.jce.provider.BouncyCastleProvider;

/**
 * This application processes input telemetry from the mbed-ethernet-sample project (monotonic counter resource) and sends a message back to 
 * the device (via IoTEventHub and the mbed Connector bridge) to toggle the LED resource on or off depending on whether the counter resource
 * is even or odd
 */
public class IoTEventHubResponder {

    private static EventHubClient client;
    private static long now = System.currentTimeMillis();
  
    // Configuration/Connection Parameters - you need to change these
    public static final String connectionString = "[Enter your IoTHub OWNER Connection String here]";
    public static final String deviceId = "[mbed Connector Endpoint Name goes here]";
    public static final String policyKey = "[SharedAccessKey part of your OWNER SAS Token goes here... you leave off the SharedAccessKey= though though...]";
    public static final String namespace = "[Your IoTHub namespace value]";
    public static final String name = "[Your IoTHub qualified name goes here]";
    public static final String dm_fota_data = "{}";                                         //FOTA manifest/image
    
    // You should not have to change these... 
    public static final String policyName = "iothubowner";
    public static final String counter_resource_uri = "/123/0/4567";
    public static final String accelerometer_resource_uri = "/888/0/7700";
    public static final String led_resource_uri = "/311/0/5850";
    public static final String temp_resource_uri = "/303/0/5700";
    
    // Device Management URIs
    public static final String dm_passphrase = "arm1234";                                               // Passphrase for permitting Actions... see main.cpp in endpoint
    public static final String dm_firmware_version_resource_uri = "/3/0/3";                             // Firmware Version
    public static final String dm_deregister_action_resource_uri = "/3/0/86";                           // Action: De-Register device
    public static final String dm_reboot_action_resource_uri = "/3/0/7";                                // Action: Reboot device
    public static final String dm_reset_action_resource_uri = "/3/0/8";                                 // Action: Reset device
    public static final String dm_fota_manifest_resource_uri = "/5/0/1";                                // Manifest: FOTA URL
    public static final String dm_fota_action_resource_uri = "/5/0/2";                                  // Action: FOTA device
    
    // sleep time between receives (MS)
    public static final int receiveSleepMS = 1000;      // 1 second
    
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
   
    // Message Receiver
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
        
        // send CoAP GET request for a given URL
        private void dispatchGETRequest(String ep_name,String uri) {
            // CoAP Verb: GET
            String coap_verb = "get";
            
            // Temperature JSON message for the bridge to parse...
            String message = "{ \"path\":\"" + uri + "\", \"ep\":\"" + ep_name + "\", \"coap_verb\": \""+ coap_verb + "\" }";

            // Send this message
            this.sendMessage(coap_verb,ep_name,message);
        }
        
         // send CoAP POST command to the special device management resources to invoke actions
        private void dispatchDeviceManagementAction(String ep_name,String uri,String passphrase) {
            // default CoAP verb is POST
            this.dispatchDeviceManagementAction(ep_name, uri, passphrase, "post");
        }
        
        // send CoAP POST command to the special device management resources to invoke actions
        private void dispatchDeviceManagementAction(String ep_name,String uri,String passphrase,String coap_verb) {
            // DEBUG Add 10 to Counter
            System.out.println("Invoking (" + coap_verb + ") Action to: " + ep_name + " URI: " + uri);
            
            // Add 10 to Counter JSON message for bridge to parse (new_value is the passphrase to permit the action)
            String message = "{ \"path\":\"" + uri + "\",\"new_value\": \""+ passphrase +"\",\"ep\":\"" + ep_name + "\", \"coap_verb\": \""+ coap_verb + "\" }";

            // Send this message
            this.sendMessage(coap_verb,ep_name,message);
        }
        
        // Device Management: De-Register the device
        private void deregisterDevice(String ep_name,String passphrase) {
            this.dispatchDeviceManagementAction(ep_name,dm_deregister_action_resource_uri,passphrase);
        }
        
        // Device Management: Reboot the device
        private void rebootDevice(String ep_name,String passphrase) {
            this.dispatchDeviceManagementAction(ep_name,dm_reboot_action_resource_uri,passphrase);
        }
        
        // Device Management: Reset the device
        private void resetDevice(String ep_name,String passphrase) {
            this.dispatchDeviceManagementAction(ep_name,dm_reset_action_resource_uri,passphrase);
        }
        
        // Device Management: Get the current Firmware version from the device
        private void getDeviceFirmwareVersion(String ep_name) {
            // dispatch the GET.. answer will come back via observation/notification response...
            this.dispatchGETRequest(ep_name,dm_firmware_version_resource_uri);
        }
        
        // Device Management: FOTA the device: set manifest
        private void fotaSetManifest(String ep_name,String fota_manifest,String passphrase) {
            // first we PUT the manifest (via CoAP PUT)
            this.dispatchDeviceManagementAction(ep_name,dm_fota_manifest_resource_uri,fota_manifest,"put");
            
            // we now wait for the PUT to complete... then we will POST to execute... see processPutResponse()
        }
        
        // Device Management: FOTA the device: invoke
        private void fotaInvokeFOTA(Map response) {
        // Check for FOTA invocation readiness...
            String uri = (String)response.get("path");
            if (uri != null && uri.equalsIgnoreCase(dm_fota_manifest_resource_uri)) {
                // DEBUG
                System.out.println("Invoking FOTA(POST) for Device: " + (String)response.get("ep"));
                
                // then we POST to invoke the FOTA action using the passphrase to permit the action
                this.dispatchDeviceManagementAction((String)response.get("ep"),dm_fota_action_resource_uri,dm_passphrase);
            }
        }
        
        // send CoAP POST to add 10 to the counter: "Add 10 to Counter"
        private void dispatchAdd10ToCounter(String ep_name,String uri) {
            // DEBUG Add 10 to Counter
            System.out.println("Add 10 to Counter: " + ep_name + " URI: " + uri);
            
            // CoAP Verb: POST
            String coap_verb = "post";
            
            // Add 10 to Counter JSON message for bridge to parse
            String message = "{ \"path\":\"" + uri + "\",\"new_value\": \"10\",\"ep\":\"" + ep_name + "\", \"coap_verb\": \""+ coap_verb + "\" }";

            // Send this message
            this.sendMessage(coap_verb,ep_name,message);
        }
        
        // send CoAP PUT to set the counter to 8: "Set Counter to 8"
        private void dispatchSetCounterTo8(String ep_name,String uri) { 
            // DEBUG Set Counter to 8
            System.out.println("Setting Counter to 8: " + ep_name + " URI: " + uri);
            
            // Set Counter to 8
            this.dispatchSetCounterToValue(ep_name, uri, "8"); 
        }
        
        // send CoAP PUT to set the counter to : "Reset Counter"
        private void dispatchResetCounter(String ep_name,String uri) { 
            // DEBUG Reset Counter
            System.out.println("Resetting Counter: " + ep_name + " URI: " + uri);
            
            // Set Counter to 0
            this.dispatchSetCounterToValue(ep_name, uri, "0"); 
        }
        
        // send CoAP PUT to set the counter to a new value
        private void dispatchSetCounterToValue(String ep_name,String uri,String value) {
            // CoAP Verb: GET
            String coap_verb = "put";
            
            // Temperature JSON message for the bridge to parse...
            // { "path":"/123/0/4567", "new_value":"8", "ep":"cc69e7c5-c24f-43cf-8365-8d23bb01c707", "coap_verb":"put" }
            String message = "{ \"path\":\"" + uri + "\",\"new_value\": \"" + value + "\",\"ep\":\"" + ep_name + "\", \"coap_verb\": \""+ coap_verb + "\" }";

            // Send this message
            this.sendMessage(coap_verb,ep_name,message);
        }
        
        // process a CoAP GET Response
        @SuppressWarnings("empty-statement")
        private void processGetResponse(Map response) {
            // DEBUG
            System.out.println("GET RESPONSE: " + response);
            
            // Do fun and interesting stuff...
            ;
        }
        
        // process a CoAP PUT Response
        @SuppressWarnings("empty-statement")
        private void processPutResponse(Map response) {
            // DEBUG
            System.out.println("PUT RESPONSE: " + response);

            // check and Process FOTA readiness
            fotaInvokeFOTA(response);
        }
        
        // process a CoAP Observation
        private void processObservation(Map observation) {
            boolean led_off_request_temp = false;
            
            // DEBUG We are processing an observation
            System.out.println("Processing Observation: " + observation);

            // Get the path... if it is the monotonic counter resource, we will process it... 
            String path = (String)observation.get("path");
            if (path != null && path.contentEquals(IoTEventHubResponder.accelerometer_resource_uri) == true) {
                try {
                    // Get the Accelerometer JSON object
                    Map accel_json = (Map)observation.get("value");
                    
                    // DEBUG
                    System.out.println("processObservation: ACCEL: " + accel_json);
                }
                catch (Exception ex) {
                    System.out.println("processObservation: Exception: " + ex.getMessage());
                }
            }
            
            if (path != null && path.contentEquals(IoTEventHubResponder.counter_resource_uri) == true) {
                try {
                    // get the monotonic counter value (an integer) (quick-json has an issue of always using Strings...) 
                    String counter_str = (String)observation.get("value");
                    Integer counter = Integer.parseInt(counter_str);
                   
                    // CoAP put() verb will be used
                    HashMap<String,String> messageProperties = new HashMap<>();
                    messageProperties.put("coap_verb", "put");

                    // if the counter is even, turn LED ON... otherwise, turn LED OFF.. we can set the CoAP verb in the JSON itself...
                    String commandMessage = null;
                    if (counter%2 == 0) {
                        // DEBUG
                        System.out.println("processObservation: Turning LED ON");

                        // turn ON
                        commandMessage = "{ \"path\":\"" + led_resource_uri + "\", \"new_value\":\"1\", \"ep\":\"" + deviceId + "\", \"coap_verb\": \""+ messageProperties.get("coap_verb") + "\" }";
                    
                        // we will issue a GET on the temperature directly (simply as an example of issuing a get())
                        led_off_request_temp = false;
                    }
                    else {
                        // DEBUG
                        System.out.println("processObservation: Turning LED OFF");

                        // turn OFF
                        commandMessage = "{ \"path\":\"" + led_resource_uri + "\", \"new_value\":\"0\", \"ep\":\"" + deviceId + "\", \"coap_verb\": \""+ messageProperties.get("coap_verb") + "\" }";

                        // we will issue a GET on the temperature directly (simply as an example of issuing a get())
                        led_off_request_temp = true;
                    }

                    // DEBUG
                    System.out.println("processObservation: Sending LED message: " + commandMessage + " to device: " + deviceId);
                    
                    // send the message
                    this.sendMessage("put", deviceId, commandMessage);

                    // ***** NodeRED Flow processing *****
                    
                    // now do a GET on temperature if the counter value is ODD
                    if (led_off_request_temp == true) {
                        // DEBUG
                        System.out.println("processObservation: Dispatching CoAP GET of Temperature resource from device: " + deviceId);
                        this.dispatchGETRequest(deviceId,temp_resource_uri);
                    }
                    
                    // Get the Firmware Version
                    if (counter == 3) {
                        // DEBUG
                        System.out.println("processObservation: Dispatching CoAP GET for Firmware Version of the device: " + deviceId);
                        this.getDeviceFirmwareVersion(deviceId);
                    }
                    
                    // Check the counter value
                    if (counter  == 5) {
                        // Set Counter Value to 8
                        this.dispatchSetCounterTo8(deviceId, counter_resource_uri);
                        
                        // TEST: Data Management actions 
                        //this.deregisterDevice(deviceId, dm_passphrase);
                        //this.rebootDevice(deviceId, dm_passphrase);
                        //this.resetDevice(deviceId, dm_passphrase);
                        this.fotaSetManifest(deviceId,dm_fota_data,dm_passphrase);
                    }
                    
                    if (counter == 10) {
                        // Add 10 to Counter
                        this.dispatchAdd10ToCounter(deviceId, counter_resource_uri);
                    }
                    
                    if (counter > 22) {
                        // Reset Counter
                        this.dispatchResetCounter(deviceId, counter_resource_uri);
                        
                        // TEST: Data Management actions 
                        //this.deregisterDevice(deviceId, dm_passphrase);
                        //this.rebootDevice(deviceId, dm_passphrase);
                        //this.resetDevice(deviceId, dm_passphrase);
                        //this.fotaSetManifest(deviceId,dm_fota_data,dm_passphrase);
                    }
                    
                    // ***** NodeRED Flow processing *****
                }
                catch (NumberFormatException ex) {
                    System.out.println("processObservation: Exception: " + ex.getMessage());
                }
            }
        }
        
        // send a message
        private void sendMessage(String verb,String ep_name, String commandMessage) {
            try {
                // CoAP GET requested
                HashMap<String,String> messageProperties = new HashMap<>();
                messageProperties.put("coap_verb", verb);
                
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
                CompletableFuture<Void> completableFuture = serviceClient.sendAsync(ep_name, messageToSend);
                completableFuture.get();
            }
            catch (UnsupportedEncodingException | InterruptedException | ExecutionException ex) {
                System.out.println("sendMessage: Exception: " + ex.getMessage());
            }
        }
        
        // process an input event
        private void processEvent(String message) {
            // parse the message
            Map parsed = this.parseMessage(message.replace("\"\"","\" \""));    // jsonParser has issues... 
            
            // Get the CoAP verb
            String verb = (String)parsed.get("verb");
            if (verb != null && verb.equalsIgnoreCase("get") == true) {
                // We are processing a GET response
                this.processGetResponse(parsed);
            }
            else if (verb != null && verb.equalsIgnoreCase("put") == true) {
                // We are processing a PUT response
                this.processPutResponse(parsed);
            }
            else {
                // We are processing an observation (DEFAULT)
                this.processObservation(parsed);
            }
        }

        // parse the JSON string
        private Map parseMessage(String json) { return this.m_json_parser.parseJson(json); }
        
        @Override
        public void run() {
            try {
                EventHubReceiver receiver = client.getConsumerGroup(null).createReceiver(partitionId, new EventHubEnqueueTimeFilter(now), Constants.DefaultAmqpCredits);
                System.out.println("Created receiver on partition " + partitionId);
                while (!stopThread) {
                    EventHubMessage message = EventHubMessage.parseAmqpMessage(receiver.receive(receiveSleepMS));
                    if (message != null) {
                        // DEBUG
                        //System.out.println("Received: " + partitionId + " (" + message.getOffset() + " | "
                        //        + message.getSequence() + " | " + message.getEnqueuedTimestamp()
                        //        + ") => " + message.getDataAsString());
                        
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
        // enable PEMReader() from BouncyCastle
        java.security.Security.addProvider(new BouncyCastleProvider());
        
        try {
            ConnectionStringBuilder csb = new ConnectionStringBuilder(IoTEventHubResponder.policyName, IoTEventHubResponder.policyKey, IoTEventHubResponder.namespace);
            IoTEventHubResponder.client = EventHubClient.create(csb.getConnectionString(), IoTEventHubResponder.name);
        }
        catch (EventHubException e) {
            System.out.println("Exception: " + e.getMessage());
        }
        
        // DEBUG Announcement
        System.out.println("Starting IoTEventHubResponder. Listening for Device: " + IoTEventHubResponder.deviceId);

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
