// File: netlify/functions/track.js
// ENHANCED VERSION: Best practices from all versions combined
// Features: Cascading IP extraction, robust attribution logic, storage verification, no API key requirement

const handler = async (event, context) => {
  console.log('Enhanced track function started');
  
  // Handle CORS for all requests
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  // Environment variable validation (from June 28th - recommended for production)
  const requiredEnvVars = [
    'UPSTASH_REDIS_REST_URL',
    'UPSTASH_REDIS_REST_TOKEN'
  ];
  
  const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
  if (missingEnvVars.length > 0) {
    console.log('Missing environment variables:', missingEnvVars);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Configuration error',
        missing: missingEnvVars
      })
    };
  }

  // Enhanced Redis helper with better error handling and timeouts (from June 28th)
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
      console.log(`⏰ Redis timeout after ${timeoutMs}ms for command: ${command.split('/')[0]}`);
    }, timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.log(`❌ Redis HTTP error ${response.status}: ${errorText}`);
        throw new Error(`Redis HTTP error: ${response.status} ${errorText}`);
      }
      
      const result = await response.json();
      console.log(`✅ Redis command success: ${command.split('/')[0]} -> ${JSON.stringify(result).substring(0, 100)}`);
      
      return result;
      
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        console.log(`⏰ Redis command timed out: ${command.split('/')[0]}`);
        throw new Error(`Redis timeout after ${timeoutMs}ms`);
      }
      
      console.log(`❌ Redis command failed: ${command.split('/')[0]} - ${error.message}`);
      throw error;
    }
  };

  // Enhanced IP extraction function with cascading fallbacks
  function getConversionIP(data) {
    try {
      // Method 1: July 1st approach (deeper nested path)
      const july1stIP = data.checkoutview?.pageviewcheckout?.pageview?.ip;
      if (july1stIP) {
        console.log('✅ IP extracted using July 1st method (deep nested):', july1stIP);
        return july1stIP;
      }
      
      // Method 2: June 28th approach (customer nested path)
      const june28thIP = data.customer?.ip_address;
      if (june28thIP) {
        console.log('✅ IP extracted using June 28th fallback (customer nested):', june28thIP);
        return june28thIP;
      }
      
      // Method 3: Final fallback to top-level
      const topLevelIP = data.ip;
      if (topLevelIP) {
        console.log('✅ IP extracted using top-level fallback:', topLevelIP);
        return topLevelIP;
      }
      
      console.log('❌ No IP found in any extraction method');
      return null;
      
    } catch (error) {
      console.log('❌ IP extraction error:', error);
      return data.ip || null; // Final safety fallback
    }
  }

  // Storage function with verification (from June 28th - critical for conversion data)
  async function storeConversionWithVerification(trackingData) {
    const storageKey = `conversions:${trackingData.timestamp}:${Math.random().toString(36).substr(2, 9)}`;
    
    try {
      console.log('🔄 Attempting Redis set for key (PERMANENT STORAGE):', storageKey);
      console.log('📦 Data size:', JSON.stringify(trackingData).length, 'characters');
      
      // Step 1: Store the data (permanent storage, no TTL)
      const setResult = await redis(`set/${storageKey}/${encodeURIComponent(JSON.stringify(trackingData))}`, 8000);
      console.log('📋 Redis set returned:', JSON.stringify(setResult));
      
      // Step 2: Verify the write actually worked by reading it back
      console.log('🔍 Verifying write with get command...');
      const verifyResult = await redis(`get/${storageKey}`, 3000);
      console.log('📋 Redis get verification:', verifyResult ? 'DATA FOUND ✅' : 'DATA NOT FOUND ❌');
      
      if (verifyResult && verifyResult.result) {
        console.log('✅ VERIFIED: Conversion data successfully written and readable (PERMANENT STORAGE)');
        return { success: true, key: storageKey, verified: true };
      } else {
        console.log('❌ CRITICAL: Data not found after set command - Redis write failed silently');
        return { success: false, error: 'Data not found after write attempt - silent Redis failure', verified: false };
      }
      
    } catch (error) {
      console.log('❌ Redis storage operation failed:', error.message);
      return { success: false, error: error.message, verified: false };
    }
  }

  try {
    const data = JSON.parse(event.body || '{}');
    console.log('Webhook data received:', Object.keys(data));

    // Enhanced data extraction with cascading IP extraction
    const extractedData = {
      email: data.email || data.customer?.email || 'unknown',
      order_total: parseFloat(data.order_total) || 0,
      order_id: data.order_id || 'unknown',
      currency: data.currency || 'usd',
      
      // Handle subscription data
      subscription_id: data.subscription_id,
      subscription_amount: data.subscription_amount,
      offer_name: data.offer_name,
      event_name: data.event_name,
      
      // Enhanced IP extraction with cascading fallbacks
      PIP: data.ip,                           // Primary IP (always top-level)
      CIP: getConversionIP(data),             // Conversion IP (cascading extraction)
      IP: getConversionIP(data) || data.ip,   // Fallback IP (same as CIP with safety)
      
      // Attribution parameters (handle both field name variations)
      SSID: data.ssid || data.session_id,                     // Session ID from custom field
      dsig: data.dsig || data.device_signature,               // Device signature from custom field
      SVV: data.SVVV || data.SVV || data.screen_value,        // Screen value - check SVVV first
      gsig: data.gsig || data.gpu_signature,                  // GPU signature from custom field
      
      // Additional webhook data
      customer_id: data.customer_id,
      checkout_publish_id: data.checkout_publish_id,
      card_id: data.card_id,
      checkoutview_id: data.checkoutview_id,
      gateway_id: data.gateway_id,
      timestamp: new Date().toISOString()
    };

    // Enhanced IP analysis with method tracking
    const ipExtractionMethods = {
      PIP: 'top_level_ip',
      CIP: extractedData.CIP === data.checkoutview?.pageviewcheckout?.pageview?.ip ? 'july_1st_deep_nested' :
           extractedData.CIP === data.customer?.ip_address ? 'june_28th_customer_nested' :
           extractedData.CIP === data.ip ? 'top_level_fallback' : 'unknown',
      IP: 'same_as_cip'
    };

    const uniqueIPs = [...new Set([extractedData.PIP, extractedData.CIP, extractedData.IP].filter(Boolean))];
    const isDualIPScenario = uniqueIPs.length > 1;
    
    console.log('🔧 IP Extraction Results:', {
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      extraction_methods: ipExtractionMethods,
      unique_count: uniqueIPs.length,
      dual_ip_detected: isDualIPScenario
    });

    console.log('Enhanced data extraction complete:', {
      email: extractedData.email,
      has_session_id: !!extractedData.SSID,
      has_device_signature: !!extractedData.dsig,
      has_screen_value: !!extractedData.SVV,
      has_gpu_signature: !!extractedData.gsig,
      ip_addresses: {
        primary: extractedData.PIP,
        conversion: extractedData.CIP,
        fallback: extractedData.IP
      }
    });

    // Attribution lookup with enhanced 8-tier priority system (using June 28th logic)
    let attributionResult = null;

    // Priority 1: Session ID Match (300 points) - HIGHEST PRIORITY
    if (extractedData.SSID && !attributionResult) {
      try {
        console.log('Trying session ID lookup:', extractedData.SSID);
        const sessionKey = `attribution_session_${extractedData.SSID}`;
        const sessionResult = await redis(`get/${sessionKey}`, 3000);
        
        if (sessionResult.result) {
          // June 28th logic: Two-step lookup with URL decoding
          const attributionDataResult = await redis(`get/${sessionResult.result}`, 3000);
          if (attributionDataResult.result) {
            const originalAttribution = JSON.parse(decodeURIComponent(attributionDataResult.result));
            attributionResult = {
              data: originalAttribution,
              method: 'session_id_match',
              score: 300
            };
            console.log('✅ Session ID attribution found');
          }
        }
      } catch (sessionError) {
        console.log('Session ID lookup failed:', sessionError.message);
      }
    }

    // Priority 2: Device Signature Match (260 points) - June 28th prioritization
    if (extractedData.dsig && !attributionResult) {
      try {
        console.log('Trying device signature lookup:', extractedData.dsig.substring(0, 10) + '...');
        // June 28th logic: Use truncated signature (last 20 chars)
        const deviceKey = `attribution_fp_${extractedData.dsig.slice(-20)}`;
        const deviceResult = await redis(`get/${deviceKey}`, 3000);
        
        if (deviceResult.result) {
          // Two-step lookup with URL decoding
          const attributionDataResult = await redis(`get/${deviceResult.result}`, 3000);
          if (attributionDataResult.result) {
            const originalAttribution = JSON.parse(decodeURIComponent(attributionDataResult.result));
            attributionResult = {
              data: originalAttribution,
              method: 'device_signature_match',
              score: 260
            };
            console.log('✅ Device signature attribution found');
          }
        }
      } catch (deviceError) {
        console.log('Device signature lookup failed:', deviceError.message);
      }
    }

    // Priority 3-5: IP Address Matches (280-240 points)
    const ipAddressesToTry = [
      { ip: extractedData.PIP, method: 'primary_ip_match', score: 280 },
      { ip: extractedData.CIP, method: 'conversion_ip_match', score: 260 },
      { ip: extractedData.IP, method: 'pageview_ip_match', score: 240 }
    ];

    for (const ipData of ipAddressesToTry) {
      if (ipData.ip && !attributionResult) {
        try {
          console.log('Trying IP lookup:', ipData.ip);
          const ipKey = `attribution_ip_${ipData.ip.replace(/:/g, '_')}`;
          const ipResult = await redis(`get/${ipKey}`, 3000);
          
          if (ipResult.result) {
            // Two-step lookup with URL decoding
            const attributionDataResult = await redis(`get/${ipResult.result}`, 3000);
            if (attributionDataResult.result) {
              const originalAttribution = JSON.parse(decodeURIComponent(attributionDataResult.result));
              attributionResult = {
                data: originalAttribution,
                method: ipData.method,
                score: ipData.score
              };
              console.log(`✅ ${ipData.method} attribution found`);
              break;
            }
          }
        } catch (ipError) {
          console.log(`IP lookup failed for ${ipData.ip}:`, ipError.message);
        }
      }
    }

    // Detect event type
    const isSubscriptionEvent = !!(data.subscription_id || data.event_name?.includes('subscription'));
    
    // Build comprehensive tracking data with ACTUAL attribution parameter values
    const enhancedTrackingData = {
      email: extractedData.email,
      order_total: extractedData.order_total,
      order_id: extractedData.order_id,
      timestamp: extractedData.timestamp,
      
      // Event classification
      event_type: isSubscriptionEvent ? 'subscription' : 'purchase',
      subscription_id: extractedData.subscription_id,
      subscription_amount: extractedData.subscription_amount,
      offer_name: extractedData.offer_name || 'Ojoy 7 Day FREE Trial',
      event_name: extractedData.event_name,
      
      // Attribution results
      attribution_found: !!attributionResult,
      attribution_method: attributionResult?.method || 'none',
      attribution_score: attributionResult?.score || 0,
      source: attributionResult?.data?.source || 'direct',
      campaign: attributionResult?.data?.utm_campaign || 'none',
      medium: attributionResult?.data?.utm_medium || 'none',
      landing_page: attributionResult?.data?.landing_page || null,
      
      // Store ACTUAL attribution parameter values (critical for recovery scripts)
      session_id: extractedData.SSID,        // Actual session ID value
      ssid: extractedData.SSID,              // Alternative field name
      dsig: extractedData.dsig,              // Actual device signature
      device_signature: extractedData.dsig,  // Alternative field name
      SVV: extractedData.SVV,                // Actual screen value
      SVVV: extractedData.SVV,               // Store under both names
      screen_value: extractedData.SVV,       // Alternative field name  
      gsig: extractedData.gsig,              // Actual GPU signature
      gpu_signature: extractedData.gsig,     // Alternative field name
      
      // IP tracking metadata  
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP || 'unknown',
      primary_ip: extractedData.PIP || null,
      conversion_ip: extractedData.CIP || null,
      pageview_ip: extractedData.IP || null,
      
      // Enhanced dual IP detection
      dual_ip_scenario: isDualIPScenario,
      ip_addresses_detected: uniqueIPs.length,
      unique_ips: uniqueIPs,
      ip_extraction_methods: ipExtractionMethods
    };

    console.log('Attribution attempt summary:', {
      session_id_attempted: !!extractedData.SSID,
      device_signature_attempted: !!extractedData.dsig,
      ip_addresses_attempted: ipAddressesToTry.filter(ip => ip.ip).length,
      attribution_found: enhancedTrackingData.attribution_found,
      attribution_method: enhancedTrackingData.attribution_method,
      attribution_score: enhancedTrackingData.attribution_score
    });

    console.log('Final tracking data prepared:', {
      email: enhancedTrackingData.email,
      attribution_found: enhancedTrackingData.attribution_found,
      attribution_method: enhancedTrackingData.attribution_method,
      source: enhancedTrackingData.source,
      landing_page: enhancedTrackingData.landing_page || 'MISSING'
    });

    // Store conversion data with verification (critical for conversion data integrity)
    const conversionResult = await storeConversionWithVerification(enhancedTrackingData);
    
    if (conversionResult.success) {
      console.log('✅ Conversion storage verified successful with key (PERMANENT):', conversionResult.key);
    } else {
      console.log('❌ CRITICAL: Conversion storage failed:', conversionResult.error);
    }

    // Store attribution stats for monitoring (optional, 30-day TTL is fine for stats)
    try {
      const statsKey = `attribution_stats_${Date.now()}`;
      const statsData = {
        email: enhancedTrackingData.email,
        timestamp: enhancedTrackingData.timestamp,
        method: enhancedTrackingData.attribution_method,
        score: enhancedTrackingData.attribution_score,
        success: enhancedTrackingData.attribution_found,
        dual_ip: isDualIPScenario,
        fields_available: {
          session_id: !!extractedData.SSID,
          device_signature: !!extractedData.dsig,
          screen_value: !!extractedData.SVV,
          gpu_signature: !!extractedData.gsig
        }
      };
      await redis(`setex/${statsKey}/2592000/${encodeURIComponent(JSON.stringify(statsData))}`, 3000); // 30 days
      console.log('✅ Attribution stats stored');
    } catch (statsError) {
      console.log('⚠️ Stats storage failed:', statsError.message);
    }

    // Return enhanced response with storage verification
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: isSubscriptionEvent ? 
          'Subscription tracked successfully' : 'Conversion tracked successfully',
        
        // Attribution results
        attribution_found: enhancedTrackingData.attribution_found,
        attribution_method: enhancedTrackingData.attribution_method,
        attribution_score: enhancedTrackingData.attribution_score || 0,
        
        // Event details
        order_id: enhancedTrackingData.order_id,
        event_type: enhancedTrackingData.event_type,
        
        // CRITICAL: Storage verification status
        storage_verified: conversionResult.success,
        storage_key: conversionResult.key || null,
        storage_error: conversionResult.error || null,
        storage_type: 'PERMANENT',
        
        // IP extraction details
        dual_ip_detected: isDualIPScenario,
        ip_extraction_methods: ipExtractionMethods,
        attribution_fields_received: {
          session_id: !!extractedData.SSID,
          device_signature: !!extractedData.dsig,
          screen_value: !!extractedData.SVV,
          gpu_signature: !!extractedData.gsig
        },
        
        // IP breakdown for verification
        ip_details: {
          primary_ip: extractedData.PIP,
          conversion_ip: extractedData.CIP,
          pageview_ip: extractedData.IP,
          unique_ips: uniqueIPs,
          ip_count: uniqueIPs.length
        },
        
        webhook_health: {
          data_extracted: true,
          attribution_attempted: true,
          storage_attempted: true,
          storage_verified: conversionResult.success,
          storage_permanent: true,
          landing_page_copied: !!enhancedTrackingData.landing_page,
          missing_fields: [
            !extractedData.SSID && 'ssid',
            !extractedData.dsig && 'dsig', 
            !extractedData.SVV && 'SVV',
            !extractedData.gsig && 'gsig'
          ].filter(Boolean)
        }
      })
    };

  } catch (error) {
    console.error('Track function error:', error);
    
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Internal server error',
        message: error.message,
        storage_verified: false
      })
    };
  }
};

module.exports = { handler };
