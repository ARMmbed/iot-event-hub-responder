/**
 * @file    JSONParser.java
 * @brief   JSON Parser wrapper class
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.arm.connector.bridge.json;

import java.util.Map;

/**
 *
 * @author Doug Anson
 */
public class JSONParser {
    // default constructor
    public JSONParser() {
    }
    
    // parse JSON into Map/List 
    public Map parseJson(String json) {
        return com.codesnippets4all.json.parsers.JsonParserFactory.getInstance().newJsonParser().parseJson(json);
    }
}
