// File: netlify/functions/track.js
// ENHANCED VERSION with Redis write verification and proper error handling
// 🔧 UPDATED: Removed TTL for conversion data - conversions now stored permanently
// 🔧 CRITICAL FIX: Corrected IP extraction paths for Spiffy webhook structure

const handler = async (event, context) => {
  console.log('Enhanced track function started');

  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      }
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  // Validate environment variables
  const requiredEnvVars = [
    'UPSTASH_REDIS_REST_URL',
    'UPSTASH_REDIS_REST_TOKEN',
    'OJOY_API_KEY'
  ];
  
  const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
  if (missingEnvVars.length > 0) {
    console.log('Missing environment variables:', missingEnvVars);
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ 
        error: 'Configuration error',
        missing: missingEnvVars
      })
    };
  }

  // Enhanced Redis helper with better error handling and logging
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

  // 🔧 FIXED: Redis storage with verification - CONVERSION DATA NOW PERMANENT
  async function storeConversionWithVerification(trackingData) {
    const storageKey = `conversions:${trackingData.timestamp}:${Math.random().toString(36).substr(2, 9)}`;
    
    try {
      console.log('🔄 Attempting Redis set for key (PERMANENT STORAGE):', storageKey);
      console.log('📦 Data size:', JSON.stringify(trackingData).length, 'characters');
      
      // 🔧 CRITICAL CHANGE: Removed TTL - conversion data now stored permanently
      // OLD: setex/${storageKey}/86400/${data} (expired after 24 hours)
      // NEW: set/${storageKey}/${data} (permanent storage)
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

  // IPv6-safe key encoding
  function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
  }

  // 🔧 FIXED: Helper function for safe nested IP extraction
  function getNestedPageviewIP(data) {
    try {
      return data.checkoutview?.pageviewcheckout?.pageview?.ip;
    } catch (error) {
      console.log('⚠️ Error accessing nested pageview IP:', error.message);
      return null;
    }
  }

  try {
    // Parse webhook data
    const data = JSON.parse(event.body || '{}');
    console.log('Webhook data received:', Object.keys(data));

    // 🔧 CRITICAL FIX: Get nested IP safely
    const nestedPageviewIP = getNestedPageviewIP(data);

    // Extract data safely with CORRECTED field mapping for Spiffy webhook structure
    const extractedData = {
      email: data.email || data.customer?.email || 'unknown',
      order_total: parseFloat(data.order_total) || 0,
      order_id: data.order_id || 'unknown',
      
      // Handle subscription data
      subscription_id: data.subscription_id,
      subscription_amount: data.subscription_amount,
      offer_name: data.offer_name,
      event_name: data.event_name,
      
      // 🔧 FIXED: IP addresses with CORRECT Spiffy webhook paths
      PIP: data.ip,                                           // Primary IP (top-level in webhook)
      CIP: nestedPageviewIP,                                  // FIXED: Nested pageview IP from correct path
      IP: nestedPageviewIP || data.ip,                        // FIXED: Proper fallback logic
      
      // Enhanced attribution parameters (may be missing)
      SSID: data.ssid || data.session_id,                     // Session ID from custom field
      dsig: data.dsig || data.device_signature,               // Device signature from custom field
      SVV: data.SVV || data.screen_value,                     // Screen value from custom field  
      gsig: data.gsig || data.gpu_signature                   // GPU signature from custom field
    };

    console.log('🔧 FIXED - Enhanced data extraction complete:', {
      email: extractedData.email,
      has_session_id: !!extractedData.SSID,
      has_device_signature: !!extractedData.dsig,
      has_screen_value: !!extractedData.SVV,
      has_gpu_signature: !!extractedData.gsig,
      ip_addresses: {
        primary: extractedData.PIP,
        conversion: extractedData.CIP,
        fallback: extractedData.IP
      },
      webhook_structure_detected: {
        top_level_ip: !!data.ip,
        nested_pageview_ip: !!nestedPageviewIP,
        customer_object: !!data.customer,
        old_customer_ip_field: data.customer?.ip_address // Should be undefined
      }
    });

    // Detect event type
    const isSubscriptionEvent = !!(data.subscription_id || data.event_name?.includes('subscription'));
    
    // 🔧 FIXED: Detect dual IP scenario with corrected extraction
    const uniqueIPs = [...new Set([extractedData.PIP, extractedData.CIP, extractedData.IP].filter(Boolean))];
    const isDualIPScenario = uniqueIPs.length > 1;
    
    console.log('🔧 FIXED - IP Analysis:', {
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      unique_count: uniqueIPs.length,
      dual_ip_detected: isDualIPScenario,
      unique_ips_list: uniqueIPs,
      fix_status: nestedPageviewIP ? 'NESTED_IP_FOUND ✅' : 'NESTED_IP_MISSING ⚠️'
    });

    // 8-Tier Attribution System
    let attributionResult = null;

    // Priority 1: Session ID Match (300 points) - HIGHEST PRIORITY
    if (extractedData.SSID && !attributionResult) {
      try {
        console.log('Trying session ID lookup:', extractedData.SSID);
        const sessionKey = `attribution_session_${extractedData.SSID}`;
        const sessionResult = await redis(`get/${sessionKey}`, 3000);
        
        if (sessionResult.result) {
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

    // Priority 2: Device Signature Match (260 points)
    if (extractedData.dsig && !attributionResult) {
      try {
        console.log('Trying device signature lookup:', extractedData.dsig.substring(0, 10) + '...');
        const deviceKey = `attribution_fp_${extractedData.dsig.slice(-20)}`;
        const deviceResult = await redis(`get/${deviceKey}`, 3000);
        
        if (deviceResult.result) {
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

    // Priority 3 & 4: IP Address Matches (280-240 points) - NOW WITH CORRECTED IPs
    const ipAddressesToTry = [
      { ip: extractedData.PIP, method: 'primary_ip_match', score: 280 },
      { ip: extractedData.CIP, method: 'conversion_ip_match', score: 260 },
      { ip: extractedData.IP, method: 'pageview_ip_match', score: 240 }
    ];

    for (const ipData of ipAddressesToTry) {
      if (ipData.ip && !attributionResult) {
        try {
          console.log(`🔧 FIXED - Trying IP lookup: ${ipData.method} with IP:`, ipData.ip);
          const ipKey = `attribution_ip_${encodeIPForKey(ipData.ip)}`;
          console.log(`🔧 FIXED - Redis key for ${ipData.method}:`, ipKey);
          const ipResult = await redis(`get/${ipKey}`, 3000);
          
          if (ipResult.result) {
            const attributionDataResult = await redis(`get/${ipResult.result}`, 3000);
            if (attributionDataResult.result) {
              const originalAttribution = JSON.parse(decodeURIComponent(attributionDataResult.result));
              attributionResult = {
                data: originalAttribution,
                method: ipData.method,
                score: ipData.score
              };
              console.log(`✅ ${ipData.method} attribution found with IP: ${ipData.ip}`);
              break;
            }
          } else {
            console.log(`⚠️ No attribution found for ${ipData.method} with IP: ${ipData.ip}`);
          }
        } catch (ipError) {
          console.log(`IP lookup failed for ${ipData.ip}:`, ipError.message);
        }
      }
    }

    // Build comprehensive tracking data
    const enhancedTrackingData = {
      email: extractedData.email,
      order_total: extractedData.order_total,
      order_id: extractedData.order_id,
      timestamp: new Date().toISOString(),
      
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
      
      // Attribution field availability (for debugging/analytics)
      attribution_fields_present: {
        session_id: !!extractedData.SSID,
        device_signature: !!extractedData.dsig,
        screen_value: !!extractedData.SVV,
        gpu_signature: !!extractedData.gsig
      },
      
      // 🔧 FIXED: IP tracking metadata with corrected values
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP || 'unknown',
      primary_ip: extractedData.PIP || null,
      conversion_ip: extractedData.CIP || null,
      pageview_ip: extractedData.IP || null,
      
      // Enhanced dual IP detection (now should work correctly)
      dual_ip_scenario: isDualIPScenario,
      ip_addresses_detected: uniqueIPs.length,
      unique_ips: uniqueIPs,
      
      // 🔧 NEW: IP extraction status for debugging
      ip_extraction_status: {
        nested_ip_found: !!nestedPageviewIP,
        webhook_path_used: 'data.checkoutview.pageviewcheckout.pageview.ip',
        old_customer_ip_path: data.customer?.ip_address, // Should be undefined
        extraction_successful: isDualIPScenario
      }
    };

    console.log('🔧 FIXED - Attribution attempt summary:', {
      session_id_attempted: !!extractedData.SSID,
      device_signature_attempted: !!extractedData.dsig,
      ip_addresses_attempted: ipAddressesToTry.filter(ip => ip.ip).length,
      unique_ips_to_try: uniqueIPs,
      attribution_found: enhancedTrackingData.attribution_found,
      attribution_method: enhancedTrackingData.attribution_method,
      attribution_score: enhancedTrackingData.attribution_score
    });

    console.log('Final tracking data prepared:', {
      email: enhancedTrackingData.email,
      attribution_found: enhancedTrackingData.attribution_found,
      attribution_method: enhancedTrackingData.attribution_method,
      source: enhancedTrackingData.source,
      landing_page: enhancedTrackingData.landing_page || 'MISSING',
      dual_ip_scenario: enhancedTrackingData.dual_ip_scenario,
      ip_count: enhancedTrackingData.ip_addresses_detected
    });

    // ENHANCED: Store conversion data with verification (NOW WITH PERMANENT STORAGE)
    const conversionResult = await storeConversionWithVerification(enhancedTrackingData);
    
    if (conversionResult.success) {
      console.log('✅ Conversion storage verified successful with key (PERMANENT):', conversionResult.key);
    } else {
      console.log('❌ CRITICAL: Conversion storage failed:', conversionResult.error);
    }

    // Store attribution stats for monitoring (30-day TTL is fine for stats)
    try {
      const statsKey = `attribution_stats_${Date.now()}`;
      const statsData = {
        email: enhancedTrackingData.email,
        timestamp: enhancedTrackingData.timestamp,
        method: enhancedTrackingData.attribution_method,
        score: enhancedTrackingData.attribution_score,
        success: enhancedTrackingData.attribution_found,
        dual_ip: isDualIPScenario,
        fields_available: enhancedTrackingData.attribution_fields_present,
        ip_extraction_fixed: !!nestedPageviewIP
      };
      await redis(`setex/${statsKey}/2592000/${encodeURIComponent(JSON.stringify(statsData))}`, 3000); // 30 days (kept TTL for stats)
      console.log('✅ Attribution stats stored');
    } catch (statsError) {
      console.log('⚠️ Stats storage failed:', statsError.message);
    }

    // Return enhanced response with storage verification
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
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
        storage_type: 'PERMANENT', // NEW: Indicates no TTL
        
        // 🔧 FIXED: Additional debugging info with IP extraction status
        dual_ip_detected: isDualIPScenario,
        attribution_fields_received: enhancedTrackingData.attribution_fields_present,
        
        // 🔧 FIXED: IP breakdown for verification
        ip_details: {
          primary_ip: extractedData.PIP,
          conversion_ip: extractedData.CIP,
          pageview_ip: extractedData.IP,
          unique_ips: uniqueIPs,
          ip_count: uniqueIPs.length,
          nested_ip_extraction_successful: !!nestedPageviewIP,
          webhook_path_working: 'data.checkoutview.pageviewcheckout.pageview.ip'
        },
        
        webhook_health: {
          data_extracted: true,
          attribution_attempted: true,
          storage_attempted: true,
          storage_verified: conversionResult.success, // CRITICAL FIELD
          storage_permanent: true, // NEW: Confirms no TTL
          landing_page_copied: !!enhancedTrackingData.landing_page,
          ip_extraction_fixed: !!nestedPageviewIP, // NEW: IP fix status
          dual_ip_detection_working: isDualIPScenario, // NEW: Dual IP status
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
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        error: 'Internal server error',
        message: error.message,
        storage_verified: false
      })
    };
  }
};

module.exports = { handler };
