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

package gr.ntua.ece.cslab.datasource.bda.controller.handlers.statics;

import gr.ntua.ece.cslab.datasource.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.datasource.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.datasource.bda.datastore.beans.Message;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageHandler {
    private final static Logger LOGGER = Logger.getLogger(MessageHandler.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    public static void handleMessage(Message message, String slug) throws Exception {

        String messageId = new StorageBackend(slug).insert(message);
        String messageType = message.getEntries().stream().filter(map -> map.getKey().equals("message_type")).findFirst().get().getValue();

        try {
            (new RunnerInstance(slug, messageType)).run(messageId);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "Did not launch job. "+e.getMessage());
        }
    }
}
