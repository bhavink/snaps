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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.snaplogic.account.api.Account;
import com.snaplogic.account.api.AccountType;
import com.snaplogic.account.api.AccountVariableProvider;
import com.snaplogic.account.api.ValidatableAccount;
import com.snaplogic.account.api.capabilities.AccountCategory;
import com.snaplogic.api.ExecutionException;
import com.snaplogic.common.properties.SnapProperty;
import com.snaplogic.common.properties.builders.PropertyBuilder;
import com.snaplogic.snap.api.DocumentUtility;
import com.snaplogic.snap.api.ExpressionProperty;
import com.snaplogic.snap.api.PropertyValues;
import com.snaplogic.snap.api.capabilities.General;
import com.snaplogic.snap.api.capabilities.Version;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Demonstrates Snap Accounts. Two security-sensitive properties, User ID and Passphrase, are used
 * to build a simple hash-token.
 *
 * <p>The User ID property is also made available to the Snap (through
 * the {@code account.userId} expression variable).</p>
 */
@General(title = "Redis Snap Account")
@Version(snap = 1)
@AccountCategory(type = AccountType.NONE)
public class RedisAccount implements Account<Jedis>, ValidatableAccount<Jedis>,
        AccountVariableProvider {

    private static final String REDIS_USER_ID = "redis_user_id";
    private static final String REDIS_PASSWORD = "redis_password";
    
    private static final String REDIS_CLUSTER_HOST = "redis_cluster_host";
    private static final String REDIS_HOST = "redis_host";
    private static final String REDIS_PORT = "redis_port";
    
    private static final String REDIS_CONNECTION_TIMEOUT = "redis_connection_timeOut";
    private static final String REDIS_CONNECTION_POOL = "redis_connection_pool";
	
    
    
    
    private static JedisPool jedispool = null;
    private static Jedis jedis = null;

    
    private ExpressionProperty usernameExpr;
	private ExpressionProperty  passwordExpr;
    private ExpressionProperty  clusterHostExpr;
    private ExpressionProperty  hostExpr;
    private ExpressionProperty  portExpr;
    private ExpressionProperty  connTimeOutExpr;
    private ExpressionProperty  connPoolExpr;

    
    @Override
    public void defineProperties(PropertyBuilder propertyBuilder) {
    	propertyBuilder.describe(REDIS_CLUSTER_HOST, REDIS_CLUSTER_HOST , "Cluster host separated by comma")        
        // for Enhanced Account Encryption; indicate to the SnapLogic Platform
        // that Medium/High Sensitivity-configured Organizations should encrypt
        // this data
        .expression()
        .add();
        
        propertyBuilder.describe(REDIS_HOST, REDIS_HOST, "Redis Server Host")
        .required()        
        // for Enhanced Account Encryption; indicate to the SnapLogic Platform
        // that Medium/High Sensitivity-configured Organizations should encrypt
        // this data        
        .expression()
        .add();
        
        propertyBuilder.describe(REDIS_PORT, REDIS_PORT, "Redis Server Port")
        .required()        
        // for Enhanced Account Encryption; indicate to the SnapLogic Platform
        // that Medium/High Sensitivity-configured Organizations should encrypt
        // this data
        .expression()
        .add();
        
    	propertyBuilder.describe(REDIS_USER_ID, REDIS_USER_ID, "Redis User Name")                
                // for Enhanced Account Encryption; indicate to the SnapLogic Platform
                // that Medium/High Sensitivity-configured Organizations should encrypt
                // this data
                .expression()
                .add();

        propertyBuilder.describe(REDIS_PASSWORD, REDIS_PASSWORD, "Redis user password")                
                .obfuscate() // masks user's input and sets SensitivityLevel to HIGH
                .expression()
                .add();
        
        propertyBuilder.describe(REDIS_CONNECTION_TIMEOUT, REDIS_CONNECTION_TIMEOUT, "Redis connection time out in seconds")                
        // for Enhanced Account Encryption; indicate to the SnapLogic Platform
        // that Medium/High Sensitivity-configured Organizations should encrypt
        // this data        
        .expression()
        .add();

        propertyBuilder.describe(REDIS_CONNECTION_POOL,REDIS_CONNECTION_POOL , "Redis connection pool size")                
        // for Enhanced Account Encryption; indicate to the SnapLogic Platform
        // that Medium/High Sensitivity-configured Organizations should encrypt
        // this data        
        .expression()
        .add();

        

    }

    @Override
    public void configure(PropertyValues propertyValues) {
        // Exercise: sanitize and validate
        usernameExpr = propertyValues.getAsExpression(REDIS_USER_ID);
        passwordExpr = propertyValues.getAsExpression(REDIS_PASSWORD);
        hostExpr = propertyValues.getAsExpression(REDIS_HOST);
        clusterHostExpr = propertyValues.getAsExpression(REDIS_CLUSTER_HOST);
        portExpr = propertyValues.getAsExpression(REDIS_PORT);
        connTimeOutExpr = propertyValues.getAsExpression(REDIS_CONNECTION_TIMEOUT);
        connPoolExpr = propertyValues.getAsExpression(REDIS_CONNECTION_POOL);
        /*try {
            new BigDecimal((portExpr.toString()));
        } catch (NumberFormatException e) {
            throw new ConfigurationException("INVALID PORT")
                    .withReason("PORT SHOULD NOT CONTAIN ANY CHARACTERS")
                    .withResolution("PORT MUST BE A NUMBER VALUE");
        }
        try {
            new BigDecimal((connTimeOutExpr.toString()));
        } catch (NumberFormatException e) {
            throw new ConfigurationException("INVALID CONNECTION TIMEOUT")
                    .withReason("CONNECTION TIMEOUT SHOULD NOT CONTAIN ANY CHARACTERS")
                    .withResolution("CONNECTION TIMEOUT MUST BE A NUMBER VALUE");
        }
        try {
            new BigDecimal((connPoolExpr.toString()));
        } catch (NumberFormatException e) {
            throw new ConfigurationException("INVALID CONNECTION POOL")
                    .withReason("CONNECTION POOL SHOULD NOT CONTAIN ANY CHARACTERS")
                    .withResolution("CONNECTION POOL MUST BE A NUMBER VALUE");
        }
  */  }

    @Override
    public Jedis connect() throws ExecutionException {
        try {
            
       /* 	String username = usernameExpr.eval(null);
            if (StringUtils.isBlank(username)) {
                throwExceptionEmptyProperty("USERNAME");
            }
            String password = passwordExpr.eval(null);
            if (StringUtils.isBlank(password)) {
                throwExceptionEmptyProperty("PASSWORD");
            }
            String cluster = clusterHostExpr.eval(null);
            if (StringUtils.isBlank(cluster)) {
                throwExceptionEmptyProperty("CLUSTER HOST");
            }
             String pool = connPoolExpr.eval(null);
            if (StringUtils.isBlank(pool)) {
                throwExceptionEmptyProperty("CONNECTION POOL");
            }
            
            String timeout = connTimeOutExpr.eval(null);
            if (StringUtils.isBlank(timeout)) {
                throwExceptionEmptyProperty("CONNECTION TIMEOUT");
            }
            
            
       */     
        	String host = hostExpr.eval(null);
            if (StringUtils.isBlank(host)) {
                throwExceptionEmptyProperty("HOST");
            }
            
        	
            String port = portExpr.eval(null);
            if (StringUtils.isBlank(port)) {
                throwExceptionEmptyProperty("PORT");
            }
            
           
            
        	jedis = new Jedis(host,Integer.parseInt(port));
        	
        	
        } catch (Exception e) {
            throw new ExecutionException(e, "Unable to establish connection to Redis instance")
                    .withResolution("Please make sure that Redis server is up and check connection details provided.");
        }

        return jedis;
    }

    @Override
    public void disconnect() throws ExecutionException {
        // no-op
    }

    private void throwExceptionEmptyProperty(String property) {
        throw new ExecutionException("EMPTY PROPERTY ").formatWith(property)
                .withReason("REASON_EMPTY_PROPERTY")
                .withResolution(String.format("PLEASE CHECK PROPERTY ", property));
    }

	@Override
	public Map<String, Object> getAccountVariableValue() {
		// TODO Auto-generated method stub
		return null;
	}
  

	


}