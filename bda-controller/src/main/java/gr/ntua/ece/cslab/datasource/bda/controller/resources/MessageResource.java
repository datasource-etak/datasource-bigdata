/*
 * Copyright 2022 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.datasource.bda.controller.resources;

import gr.ntua.ece.cslab.datasource.bda.controller.connectors.MessageHandler;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Message;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.MessageType;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@RestController
@RequestMapping("messages")
public class MessageResource {
    private final static Logger LOGGER = Logger.getLogger(MessageResource.class.getCanonicalName());

    /**
     * Message description insert method
     * @param m the message description to insert
     */
    @PostMapping(value = "{slug}",
            consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public ResponseEntity<?> insert(@PathVariable("slug") String slug,
                                    @RequestBody MessageType m) {
        String details;
        try {
            //if (m.getExternalConnectorId()!=null){
            //    Connector conn = Connector.getConnectorInfoById(m.getExternalConnectorId());
            //    if (!conn.isExternal())
            //        return new ResponseEntity<>("Could not insert new Message Type. Connector is not external.", HttpStatus.BAD_REQUEST);

            //    if (!conn.getMetadata().getDatasources().contains(m.getExternalDatasource()))
            //        return new ResponseEntity<>("Could not insert new Message Type. Invalid datasource specified for connector.", HttpStatus.BAD_REQUEST);

            //}
            m.save(slug);
            details = Integer.toString(m.getId());
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Could not insert new Message Type.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.ok(details);
    }

    /**
     * Returns all the registered message types.
     */
    @GetMapping(value = "{slug}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public List<MessageType> getMessageTypeView(@PathVariable("slug") String slug) {
        List<MessageType> messageTypes = new LinkedList<MessageType>();

        try {
            messageTypes = MessageType.getMessageTypes(slug);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return messageTypes;
    }

    /**
     * Returns information about a specific message type.
     */
    @GetMapping(value = "{slug}/{messageTypeId}", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public MessageType getMessageTypeInfo(@PathVariable("slug") String slug,
                                          @PathVariable("messageTypeId") Integer id) {
        MessageType messageType = null;

        try {
            messageType = MessageType.getMessageById(slug, id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return messageType;
    }

    /**
     * Delete a specific message type.
     */
    @DeleteMapping(value = "{slug}/{messageTypeId}")
    public ResponseEntity<?> deleteMessageType(@PathVariable("slug") String slug,
                                      @PathVariable("messageTypeId") Integer id) {

        try {
            MessageType.destroy(slug, id);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Could not destroy Message Type.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.ok("");
    }

    /**
     * Handle incoming PubSub message method
     * @param message the PubSub message
     * TODO: replace this rest entry with pub sub subscriber
     */
    @PostMapping(value = "{slug}/insert",
            consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE},
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public static ResponseEntity<?> handleMessage(@PathVariable("slug") String slug,
                                         @RequestBody Message message) {
        try {
            MessageHandler.handleMessage(message, slug);
            LOGGER.log(Level.INFO,"PubSub message successfully inserted in the BDA.");
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Could not insert new PubSub message.", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return ResponseEntity.ok("");
    }
}
