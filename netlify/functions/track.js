// File: netlify/functions/track.js
// PRODUCTION-READY: Handles missing custom fields gracefully with CORRECTED IP mapping

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

  // Redis helper with timeout
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` },
        signal: controller.signal
      });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };

  // IPv6-safe key encoding
  function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
  }

  try {
    // Parse webhook data
    const data = JSON.parse(event.body || '{}');
    console.log('Webhook data received:', Object.keys(data));

    // Extract data safely with comprehensive field mapping
    const extractedData = {
      email: data.email || data.customer?.email || 'unknown',
      order_total: parseFloat(data.order_total) || 0,
      order_id: data.order_id || 'unknown',
      
      // Handle subscription data
      subscription_id: data.subscription_id,
      subscription_amount: data.subscription_amount,
      offer_name: data.offer_name,
      event_name: data.event_name,
      
      // IP addresses - CORRECTED mapping based on Spiffy webhook structure
      PIP: data.ip,                                           // Primary/Original pageview IP (top-level)
      CIP: data.checkoutview?.pageviewcheckout?.pageview?.ip, // Conversion/Checkout IP (nested)
      IP: data.ip,                                            // Original pageview IP (same as PIP)
      
      // Attribution parameters - handle both field name variations and missing fields
      SSID: data.ssid,
      dsig: data.dsig,
      SVV: data.svvv || data.SVV, // Spiffy may send as 'svvv'
      gsig: data.gsig,
      
      // UTM parameters from multiple possible locations
      utm_source: data.utm_source || 
                 data.checkoutview?.utm_source ||
                 data.checkoutview?.pageviewcheckout?.pageview?.utm_source,
      utm_campaign: data.utm_campaign || 
                   data.checkoutview?.utm_campaign ||
                   data.checkoutview?.pageviewcheckout?.pageview?.utm_campaign,
      utm_medium: data.utm_medium || 
                 data.checkoutview?.utm_medium ||
                 data.checkoutview?.pageviewcheckout?.pageview?.utm_medium,
      
      timestamp: new Date().toISOString()
    };

    // Enhanced dual IP detection
    const allIPs = [extractedData.PIP, extractedData.CIP, extractedData.IP].filter(ip => ip && ip.trim() !== '');
    const uniqueIPs = [...new Set(allIPs)];
    const isDualIPScenario = uniqueIPs.length > 1;

    console.log('Extracted data:', {
      email: extractedData.email,
      order_total: extractedData.order_total,
      event_name: extractedData.event_name,
      has_SSID: !!extractedData.SSID,
      has_dsig: !!extractedData.dsig,
      has_SVV: !!extractedData.SVV,
      has_gsig: !!extractedData.gsig,
      has_PIP: !!extractedData.PIP,
      has_CIP: !!extractedData.CIP,
      has_IP: !!extractedData.IP,
      dual_ip: isDualIPScenario,
      unique_ip_count: uniqueIPs.length,
      missing_attribution_fields: [
        !extractedData.SSID && 'session_id',
        !extractedData.dsig && 'device_signature', 
        !extractedData.SVV && 'screen_value',
        !extractedData.gsig && 'gpu_signature'
      ].filter(Boolean)
    });

    // Robust attribution lookup - handles missing fields gracefully
    let attributionResult = null;

    try {
      // Priority 1: Session ID lookup (only if SSID exists)
      if (extractedData.SSID && extractedData.SSID.trim() !== '') {
        console.log('Trying session ID lookup:', extractedData.SSID);
        const sessionKey = `attribution_session_${extractedData.SSID}`;
        const sessionResult = await redis(`get/${sessionKey}`, 2000);
        
        if (sessionResult.result) {
          const attributionDataResult = await redis(`get/${sessionResult.result}`, 2000);
          if (attributionDataResult.result) {
            attributionResult = {
              data: JSON.parse(attributionDataResult.result),
              method: 'session_id_match',
              score: 300
            };
            console.log('✅ Session ID attribution found');
          }
        }
      }

      // Priority 2: Device signature lookup (only if dsig exists)
      if (!attributionResult && extractedData.dsig && extractedData.dsig.trim() !== '') {
        console.log('Trying device signature lookup:', extractedData.dsig);
        const fpKey = `attribution_fp_${extractedData.dsig}`;
        const fpResult = await redis(`get/${fpKey}`, 2000);
        
        if (fpResult.result) {
          const attributionDataResult = await redis(`get/${fpResult.result}`, 2000);
          if (attributionDataResult.result) {
            attributionResult = {
              data: JSON.parse(attributionDataResult.result),
              method: 'device_signature_match',
              score: 220
            };
            console.log('✅ Device signature attribution found');
          }
        }
      }

      // Priority 3: IP lookup (always try if IPs exist)
      if (!attributionResult && (extractedData.PIP || extractedData.CIP || extractedData.IP)) {
        // Try Primary IP first, then Conversion IP, then fallback IP
        const ipList = [extractedData.PIP, extractedData.CIP, extractedData.IP].filter(ip => ip && ip.trim() !== '');
        
        for (const testIP of ipList) {
          console.log('Trying IP lookup:', testIP);
          
          const encodedIP = encodeIPForKey(testIP);
          const ipKey = `attribution_ip_${encodedIP}`;
          
          const ipResult = await redis(`get/${ipKey}`, 2000);
          if (ipResult.result) {
            const attributionDataResult = await redis(`get/${ipResult.result}`, 2000);
            if (attributionDataResult.result) {
              attributionResult = {
                data: JSON.parse(attributionDataResult.result),
                method: testIP === extractedData.PIP ? 'primary_ip_match' : 
                        testIP === extractedData.CIP ? 'conversion_ip_match' : 'pageview_ip_match',
                score: testIP === extractedData.PIP ? 280 : 
                       testIP === extractedData.CIP ? 260 : 240
              };
              console.log(`✅ IP attribution found via ${testIP}`);
              break; // Stop on first match
            }
          }
        }
      }

      // Log attribution attempt summary
      console.log('Attribution attempt summary:', {
        session_id_attempted: !!extractedData.SSID,
        device_signature_attempted: !!extractedData.dsig,
        ip_addresses_attempted: [extractedData.PIP, extractedData.CIP, extractedData.IP].filter(Boolean).length,
        attribution_found: !!attributionResult,
        method: attributionResult?.method || 'none'
      });

    } catch (attributionError) {
      console.log('⚠️ Attribution lookup failed (non-critical):', attributionError.message);
      // Continue execution - attribution failure shouldn't break webhook processing
    }

    // Determine event type
    const isSubscriptionEvent = data.event_name === 'subscription:started' || data.subscription_id;
    const isPurchaseEvent = extractedData.order_total > 0;
    
    // Build tracking data
    const trackingData = {
      timestamp: extractedData.timestamp,
      event_type: isSubscriptionEvent ? 'subscription' : 'purchase',
      
      source: attributionResult?.data?.source || extractedData.utm_source || 'direct',
      campaign: attributionResult?.data?.utm_campaign || extractedData.utm_campaign || 'none',
      medium: attributionResult?.data?.utm_medium || extractedData.utm_medium || 'none',
      
      email: extractedData.email,
      order_id: extractedData.order_id,
      order_total: extractedData.order_total,
      
      // Subscription data (for subscription events)
      subscription_id: extractedData.subscription_id,
      subscription_amount: extractedData.subscription_amount,
      offer_name: extractedData.offer_name,
      event_name: extractedData.event_name,
      
      attribution_found: !!attributionResult,
      attribution_method: attributionResult?.method || 'none',
      attribution_score: attributionResult?.score || 0,
      
      // Attribution field availability (for debugging/analytics)
      attribution_fields_present: {
        session_id: !!extractedData.SSID,
        device_signature: !!extractedData.dsig,
        screen_value: !!extractedData.SVV,
        gpu_signature: !!extractedData.gsig
      },
      
      // IP tracking metadata  
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP || 'unknown',
      primary_ip: extractedData.PIP || null,
      conversion_ip: extractedData.CIP || null,
      pageview_ip: extractedData.IP || null,
      
      // Enhanced dual IP detection - check all IP combinations
      dual_ip_scenario: isDualIPScenario,
      ip_addresses_detected: uniqueIPs.length,
      unique_ips: uniqueIPs
    };

    console.log('Final tracking data:', trackingData);

    // Store conversion data
    try {
      const storageKey = `conversions:${trackingData.timestamp}:${Math.random().toString(36).substr(2, 9)}`;
      await redis(`setex/${storageKey}/86400/${encodeURIComponent(JSON.stringify(trackingData))}`, 2000);
      console.log('Conversion data stored');
    } catch (storageError) {
      console.log('Storage failed:', storageError.message);
    }

    // Return success
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        success: true,
        message: isSubscriptionEvent ? 'Subscription tracked successfully' : 'Conversion tracked successfully',
        attribution_found: trackingData.attribution_found,
        attribution_method: trackingData.attribution_method,
        attribution_score: trackingData.attribution_score || 0,
        order_id: trackingData.order_id,
        event_type: trackingData.event_type,
        dual_ip_detected: isDualIPScenario,
        attribution_fields_received: trackingData.attribution_fields_present,
        
        // IP breakdown for verification (can remove in production)
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
        message: error.message
      })
    };
  }
};

module.exports = { handler };
