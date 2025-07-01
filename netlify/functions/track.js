// File: netlify/functions/track.js
// CORRECTED VERSION with proper Redis key pattern: conversions:[timestamp]:[random_id]

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

  // Authentication check
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;
  
  if (!apiKey || apiKey !== validApiKey) {
    console.log('❌ Invalid or missing API key');
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid or missing API key' })
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const data = JSON.parse(event.body);
    console.log('Webhook data received:', JSON.stringify(data, null, 2));

    // 🔧 FIXED: Enhanced nested IP extraction function
    function getNestedPageviewIP(data) {
      try {
        return data.checkoutview?.pageviewcheckout?.pageview?.ip || null;
      } catch (error) {
        console.log('Error extracting nested pageview IP:', error);
        return null;
      }
    }

    // Enhanced data extraction with FIXED field mapping
    const extractedData = {
      email: data.email || 'unknown',
      order_total: data.order_total || data.amount || 0,
      currency: data.currency || 'usd',
      
      // 🔧 FIXED: Correct IP extraction paths
      PIP: data.ip,                                    // Primary IP (top-level)
      CIP: getNestedPageviewIP(data),                  // Conversion IP (nested pageview)
      IP: getNestedPageviewIP(data) || data.ip,       // Fallback IP
      
      // 🔧 FIXED: Enhanced attribution parameters with correct field names
      SSID: data.ssid || data.session_id,                          // Session ID  
      dsig: data.dsig || data.device_signature,                    // Device signature
      SVV: data.SVVV || data.SVV || data.screen_value,             // 🔧 FIXED: Check SVVV first!
      gsig: data.gsig || data.gpu_signature,                       // GPU signature
      
      // Additional webhook data
      customer_id: data.customer_id,
      checkout_publish_id: data.checkout_publish_id,
      card_id: data.card_id,
      checkoutview_id: data.checkoutview_id,
      gateway_id: data.gateway_id,
      timestamp: new Date().toISOString()
    };

    // 🔧 FIXED: Enhanced IP analysis with proper extraction
    const uniqueIPs = new Set();
    if (extractedData.PIP) uniqueIPs.add(extractedData.PIP);
    if (extractedData.CIP) uniqueIPs.add(extractedData.CIP);
    if (extractedData.IP && extractedData.IP !== extractedData.PIP) uniqueIPs.add(extractedData.IP);

    const ipAnalysis = {
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      unique_count: uniqueIPs.size,
      dual_ip_detected: uniqueIPs.size >= 2,
      unique_ips_list: Array.from(uniqueIPs),
      nested_ip_extraction_successful: !!extractedData.CIP,
      fix_status: extractedData.CIP ? 'NESTED_IP_FOUND ✅' : 'ONLY_PRIMARY_IP_FOUND ⚠️'
    };

    console.log('🔧 FIXED - IP Analysis:', ipAnalysis);

    // 🔧 FIXED: Enhanced data extraction status
    const extractionStatus = {
      has_session_id: !!extractedData.SSID,
      has_device_signature: !!extractedData.dsig,
      has_screen_value: !!extractedData.SVV,
      has_gpu_signature: !!extractedData.gsig,
      unique_count: ipAnalysis.unique_count,
      dual_ip_detected: ipAnalysis.dual_ip_detected
    };

    console.log('🔧 FIXED - Enhanced data extraction complete:', extractionStatus);

    // Upstash REST API (matches your other functions)
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    const redis = async (command) => {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      return response.json();
    };

    // Attribution lookup with 8-tier priority system
    let attributionResult = null;
    let attributionAttempts = {
      session_id_attempted: false,
      device_signature_attempted: false,
      ip_addresses_attempted: 0,
      unique_ips_to_try: Array.from(uniqueIPs),
      attribution_found: false,
      attribution_method: 'none',
      attribution_score: 0
    };

    // Priority 1: Session ID Match (300 points) - HIGHEST PRIORITY
    if (extractedData.SSID) {
      attributionAttempts.session_id_attempted = true;
      const sessionKey = `attribution_session_${extractedData.SSID}`;
      console.log(`🔧 FIXED - Trying session lookup: ${sessionKey}`);
      
      try {
        const sessionResult = await redis(`get/${sessionKey}`);
        console.log(`✅ Redis command success: get -> ${JSON.stringify({result: sessionResult})}`);
        
        if (sessionResult && sessionResult.result) {
          attributionResult = JSON.parse(sessionResult.result);
          attributionAttempts.attribution_found = true;
          attributionAttempts.attribution_method = 'session_id_match';
          attributionAttempts.attribution_score = 300;
          console.log('🎯 PRIORITY 1 SUCCESS: Session ID Match (300 points)');
        } else {
          console.log('⚠️ No attribution found for session_id_match');
        }
      } catch (error) {
        console.log('❌ Redis session lookup error:', error);
      }
    }

    // Priority 2-4: IP-based matching (if no session match found)
    if (!attributionResult && uniqueIPs.size > 0) {
      const ipPriorities = [
        { name: 'primary_ip_match', ip: extractedData.PIP, points: 280 },
        { name: 'conversion_ip_match', ip: extractedData.CIP, points: 260 },
        { name: 'pageview_ip_match', ip: extractedData.IP, points: 240 }
      ];

      for (const priority of ipPriorities) {
        if (!attributionResult && priority.ip) {
          attributionAttempts.ip_addresses_attempted++;
          const ipKey = `attribution_ip_${priority.ip.replace(/:/g, '_')}`;
          console.log(`🔧 FIXED - Trying IP lookup: ${priority.name} with IP: ${priority.ip}`);
          console.log(`🔧 FIXED - Redis key for ${priority.name}: ${ipKey}`);
          
          try {
            const ipResult = await redis(`get/${ipKey}`);
            console.log(`✅ Redis command success: get -> ${JSON.stringify({result: ipResult})}`);
            
            if (ipResult && ipResult.result) {
              attributionResult = JSON.parse(ipResult.result);
              attributionAttempts.attribution_found = true;
              attributionAttempts.attribution_method = priority.name;
              attributionAttempts.attribution_score = priority.points;
              console.log(`🎯 ${priority.name.toUpperCase()} SUCCESS: (${priority.points} points)`);
              break;
            } else {
              console.log(`⚠️ No attribution found for ${priority.name} with IP: ${priority.ip}`);
            }
          } catch (error) {
            console.log(`❌ Redis IP lookup error for ${priority.name}:`, error);
          }
        }
      }
    }

    // Priority 5: Device Signature Match (220 points)
    if (!attributionResult && extractedData.dsig) {
      attributionAttempts.device_signature_attempted = true;
      const deviceKey = `attribution_fp_${extractedData.dsig}`;
      console.log(`🔧 FIXED - Trying device signature lookup: ${deviceKey}`);
      
      try {
        const deviceResult = await redis(`get/${deviceKey}`);
        console.log(`✅ Redis command success: get -> ${JSON.stringify({result: deviceResult})}`);
        
        if (deviceResult && deviceResult.result) {
          attributionResult = JSON.parse(deviceResult.result);
          attributionAttempts.attribution_found = true;
          attributionAttempts.attribution_method = 'device_signature_match';
          attributionAttempts.attribution_score = 220;
          console.log('🎯 PRIORITY 5 SUCCESS: Device Signature Match (220 points)');
        } else {
          console.log('⚠️ No attribution found for device_signature_match');
        }
      } catch (error) {
        console.log('❌ Redis device lookup error:', error);
      }
    }

    console.log('🔧 FIXED - Attribution attempt summary:', attributionAttempts);

    // 🔧 FIXED: Enhanced tracking data with STORED attribution parameters
    const enhancedTrackingData = {
      email: extractedData.email,
      order_total: extractedData.order_total,
      currency: extractedData.currency,
      customer_id: extractedData.customer_id,
      checkout_publish_id: extractedData.checkout_publish_id,
      card_id: extractedData.card_id,
      checkoutview_id: extractedData.checkoutview_id,
      gateway_id: extractedData.gateway_id,
      timestamp: extractedData.timestamp,
      
      // 🔧 FIXED: Store IP data properly
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      
      // 🔧 FIXED: Store ALL attribution parameters for recovery scripts
      session_id: extractedData.SSID,        // For session-based recovery
      ssid: extractedData.SSID,              // Alternative field name
      dsig: extractedData.dsig,              // Device signature (truncated)
      device_signature: extractedData.dsig,  // Alternative field name
      SVV: extractedData.SVV,                // Screen value (hashed)
      SVVV: extractedData.SVV,               // Store under both names
      screen_value: extractedData.SVV,       // Alternative field name  
      gsig: extractedData.gsig,              // GPU signature (hashed)
      gpu_signature: extractedData.gsig,     // Alternative field name
      
      // Attribution results
      attribution_found: !!attributionResult,
      attribution_method: attributionAttempts.attribution_method,
      attribution_score: attributionAttempts.attribution_score,
      
      // Include attribution data if found
      ...(attributionResult && {
        source: attributionResult.source || 'direct',
        landing_page: attributionResult.landing_page || 'none',
        utm_source: attributionResult.utm_source || null,
        utm_medium: attributionResult.utm_medium || null,
        utm_campaign: attributionResult.utm_campaign || null,
        utm_content: attributionResult.utm_content || null,
        utm_term: attributionResult.utm_term || null
      }),
      
      // Default values if no attribution found
      source: attributionResult?.source || 'direct',
      landing_page: attributionResult?.landing_page || 'none'
    };

    console.log('Final tracking data prepared:', enhancedTrackingData);

    // 🔧 CORRECTED: Store using CORRECT key pattern: conversions:[timestamp]:[random_id]
    const randomId = Math.random().toString(36).substr(2, 9);
    const conversionKey = `conversions:${extractedData.timestamp}:${randomId}`;
    
    try {
      await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(enhancedTrackingData))}`);
      console.log(`✅ Conversion stored in Redis with key: ${conversionKey}`);
    } catch (error) {
      console.log('❌ Failed to store conversion in Redis:', error);
      throw error;
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Enhanced tracking completed with IP extraction fix',
        attribution_found: !!attributionResult,
        attribution_method: attributionAttempts.attribution_method,
        attribution_score: attributionAttempts.attribution_score,
        ip_analysis: ipAnalysis,
        extraction_status: extractionStatus,
        conversion_key: conversionKey
      })
    };

  } catch (error) {
    console.error('❌ Enhanced track function error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Internal server error', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
