/**
 * @file    JSONGeneratorFactory.java
 * @brief   JSON Generator Factory wrapper class
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

/**
 *
 * @author Doug Anson
 */
public class JSONGeneratorFactory {
    private static JSONGeneratorFactory me = null;
        
    // JSON Generator and Parser
    private JSONGenerator m_generator = null;
    private JSONParser m_parser = null;
    
    // generator
    public static JSONGeneratorFactory getInstance() {
        if (me == null) {
            me = new JSONGeneratorFactory();
        }
        return me;
    }
    
    // constructor
    public JSONGeneratorFactory() {
        this.m_generator = new JSONGenerator();
        this.m_parser = new JSONParser();
    }
    
    // create a json generator
    public JSONGenerator newJsonGenerator() {
        return this.m_generator;
    }
    
    // create a json parser
    public JSONParser newJsonParser() {
        return this.m_parser;
    }
}
