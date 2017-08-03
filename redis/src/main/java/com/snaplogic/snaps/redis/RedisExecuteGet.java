/*
 * SnapLogic - Data Integration
 *
 * Copyright (C) 2016, SnapLogic, Inc.  All rights reserved.
 *
 * This program is licensed under the terms of
 * the SnapLogic Commercial Subscription agreement.
 *
 * "SnapLogic" is a trademark of SnapLogic, Inc.
 */
package com.snaplogic.snaps.redis;

import java.math.BigInteger;
import com.google.inject.Inject;
import com.snaplogic.account.api.capabilities.Accounts;
import com.snaplogic.api.ConfigurationException;
import com.snaplogic.common.SnapType;
import com.snaplogic.common.properties.SnapProperty;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.Document;
import com.snaplogic.snap.api.DocumentUtility;
import com.snaplogic.snap.api.PropertyValues;
import com.snaplogic.snap.api.SimpleSnap;
import com.snaplogic.snap.api.SnapCategory;
import com.snaplogic.snap.api.SnapDataException;
import com.snaplogic.snap.api.capabilities.Category;
import com.snaplogic.snap.api.capabilities.General;
import com.snaplogic.snap.api.capabilities.Inputs;
import com.snaplogic.snap.api.capabilities.Outputs;
import com.snaplogic.snap.api.capabilities.Version;
import com.snaplogic.snap.api.capabilities.ViewType;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Snap requires configuration of an {@link ExampleAccount}.
 *
 * <p>After the account is configured, the {@code account.userId} property can be used within the
 * Snap's settings.</p>
 */
@General(title = "Redis GET", purpose = "Execute Redis GET Command",
        author = "bkukadia@snaplogic.com", docLink = "http://yourdocslinkhere.com")
@Inputs(min = 0, max = 1, accepts = {ViewType.DOCUMENT})
@Outputs(min = 1, max = 1, offers = {ViewType.DOCUMENT})
@Version(snap = 1)
@Category(snap = SnapCategory.READ)
@Accounts(provides = {RedisAccount.class}, optional = false)
public class RedisExecuteGet extends SimpleSnap {

    private static final String REDIS_COMMAND_PROP = "Redis Command";
    private static final String KEY_NAME_PROP = "Key";
    private static final String KEYS_TABLE_PROP = "Keys";
    
    // Document utility is the only way to create a document
    // or manipulate the document header
    @Inject
    private DocumentUtility documentUtility;
    
    String command;    
    List<String> keys = new ArrayList<String>();    
    private PropertyValues propertyValues;
    
    @Inject
    private RedisAccount snapAccount;
    // Once the Account has been configured, redis connection can be used in the Snap

    private static final Logger LOG = LoggerFactory.getLogger(RedisExecuteGet.class);
    
    
    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
       propertyBuilder.describe(REDIS_COMMAND_PROP, REDIS_COMMAND_PROP,
                "Select redis command to run")
                .required()
                .type(SnapType.STRING).withAllowedValues(new HashSet<String>( Arrays.asList("GET")))
                .defaultValue("GET")
                .add();
       final SnapProperty keyName = propertyBuilder
                .describe(KEY_NAME_PROP,KEY_NAME_PROP,"redis cache key")
                .type(SnapType.STRING)
                .withMinLength(1)
                .expression(SnapProperty.DecoratorType.ACCEPTS_SCHEMA)
                .expression(SnapProperty.DecoratorType.ENABLED_EXPRESSION)                
                .build();
       propertyBuilder.describe(KEYS_TABLE_PROP,KEYS_TABLE_PROP,"key value pairs to cache")
        .type(SnapType.TABLE)
        .withEntry(keyName)
        .add();
       
         
    }

    @Override
    public void configure(PropertyValues propertyValues) throws ConfigurationException {
    	this.propertyValues = propertyValues;
    }
    
   
     
    protected List<String> buildTable(final PropertyValues propertyValues,
            Document document) {
    	
        // a List of Maps to represent the evaluated property values in a table
        List<String> table = new ArrayList<String>();

        // get the table property as an expression
        List<String> tableProp = propertyValues.getAsExpression(KEYS_TABLE_PROP).eval(document);

        // for each row of the props in a table, get the child properties as an expression
        
        for(int i =0;i<tableProp.size();i++){
        	LOG.info("tablePropRaw = " + tableProp.get(i));
        	String cacheKey = propertyValues.getAsExpression(tableProp.get(i)).eval(document);
        	LOG.info("cacheKey = " + cacheKey);
        	table.add(cacheKey);
            
        }
		return table;
                   
    }

    @Override
    protected void process(Document document, String inputViewName) {
        // get a connection to redis host
    	Jedis redisConn = snapAccount.connect();
        // get list of key:value pairs
        keys = buildTable(propertyValues, document);
        // create a map that holds snap outputview data        
        Map<String, String> data = new LinkedHashMap<>();
 
        command = propertyValues.getAsExpression(REDIS_COMMAND_PROP).eval(document);
        for (int j = 0; j<getTable().size();j++) {
        	        LOG.info("Inside GET");
        	        data.put(getTable().get(j),redisConn.get(getTable().get(j)));
        	        LOG.info("Inside GET" + data);
        	        }
        
        // do something with the token

        outputViews.write(documentUtility.newDocument(data),document);
        }
    
    
    public List<String> getTable() {
        return keys;
    }

	public String getCommand() {
		return command;
	}

	
}