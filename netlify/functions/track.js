// File: netlify/functions/track.js
// FIXED: Device signature attribution now properly copies landing_page field
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
      CIP: data.customer?.ip_address,                         // Conversion/Checkout IP (nested)
      IP: data.customer?.ip_address || data.ip,               // Fallback IP
      
      // Enhanced attribution parameters (may be missing)
      SSID: data.ssid || data.session_id,                     // Session ID from custom field
      dsig: data.dsig || data.device_signature,               // Device signature from custom field
      SVV: data.SVV || data.screen_value,                     // Screen value from custom field  
      gsig: data.gsig || data.gpu_signature                   // GPU signature from custom field
    };

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

    // Detect event type
    const isSubscriptionEvent = !!(data.subscription_id || data.event_name?.includes('subscription'));
    
    // Build comprehensive tracking data
    const trackingData = {
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
      
      // Default attribution (will be enhanced if found)
      attribution_found: false,
      attribution_method: 'none',
      attribution_score: 0,
      source: 'direct',
      campaign: 'none',
      medium: 'none',
      landing_page: null,  // ← CRITICAL: This will be populated from attribution data
      
      // Attribution field availability (for debugging/analytics)
      attribution_fields_present: {
        session_id: !!extractedData.SSID,
        device_signature: !!extractedData.dsig,
        screen_value: !!extractedData.SVV,
        gpu_signature: !!extractedData.gsig
      }
    };

    // IP analysis for dual-IP detection
    const allIPs = [extractedData.PIP, extractedData.CIP, extractedData.IP].filter(ip => ip && ip.trim() !== '');
    const uniqueIPs = [...new Set(allIPs)];
    const isDualIPScenario = uniqueIPs.length > 1;
    
    console.log('IP Analysis:', {
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      unique_count: uniqueIPs.length,
      dual_ip_detected: isDualIPScenario
    });

    // ENHANCED ATTRIBUTION LOGIC - Ordered by priority
    let attributionResult = null;

    try {
      // Priority 1: Session ID lookup (highest priority - 300 points)
      if (!attributionResult && extractedData.SSID && extractedData.SSID.trim() !== '') {
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

      // Priority 2: Device signature lookup (220 points) - FIXED VERSION
      if (!attributionResult && extractedData.dsig && extractedData.dsig.trim() !== '') {
        console.log('Trying device signature lookup:', extractedData.dsig);
        const fpKey = `attribution_fp_${extractedData.dsig}`;
        const fpResult = await redis(`get/${fpKey}`, 2000);
        
        if (fpResult.result) {
          const attributionDataResult = await redis(`get/${fpResult.result}`, 2000);
          if (attributionDataResult.result) {
            const attributionData = JSON.parse(attributionDataResult.result);
            attributionResult = {
              data: attributionData,
              method: 'device_signature_match',
              score: 220
            };
            console.log('✅ Device signature attribution found');
            console.log('Attribution data includes landing_page:', !!attributionData.landing_page);
          }
        }
      }

      // Priority 3: IP lookup (variable points based on IP type)
      if (!attributionResult && (extractedData.PIP || extractedData.CIP || extractedData.IP)) {
        // Try Primary IP first (280 points), then Conversion IP (260 points), then fallback IP (240 points)
        const ipList = [
          { ip: extractedData.PIP, method: 'primary_ip_match', score: 280 },
          { ip: extractedData.CIP, method: 'conversion_ip_match', score: 260 },
          { ip: extractedData.IP, method: 'pageview_ip_match', score: 240 }
        ].filter(item => item.ip && item.ip.trim() !== '');
        
        for (const ipItem of ipList) {
          console.log('Trying IP lookup:', ipItem.ip);
          
          const encodedIP = encodeIPForKey(ipItem.ip);
          const ipKey = `attribution_ip_${encodedIP}`;
          
          const ipResult = await redis(`get/${ipKey}`, 2000);
          if (ipResult.result) {
            const attributionDataResult = await redis(`get/${ipResult.result}`, 2000);
            if (attributionDataResult.result) {
              attributionResult = {
                data: JSON.parse(attributionDataResult.result),
                method: ipItem.method,
                score: ipItem.score
              };
              console.log(`✅ IP attribution found via ${ipItem.ip} (${ipItem.method})`);
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
        attribution_method: attributionResult?.method || 'none',
        attribution_score: attributionResult?.score || 0
      });

    } catch (attributionError) {
      console.log('Attribution lookup error:', attributionError.message);
      // Continue without attribution - graceful degradation
    }

    // CRITICAL FIX: Apply attribution data to tracking record
    if (attributionResult && attributionResult.data) {
      const attrData = attributionResult.data;
      
      // ✅ FIXED: Copy ALL attribution fields including landing_page
      trackingData.attribution_found = true;
      trackingData.attribution_method = attributionResult.method;
      trackingData.attribution_score = attributionResult.score;
      trackingData.source = attrData.source || 'direct';
      trackingData.campaign = attrData.utm_campaign || attrData.campaign || 'none';
      trackingData.medium = attrData.utm_medium || attrData.medium || 'none';
      trackingData.landing_page = attrData.landing_page;  // ← CRITICAL FIX: Copy landing_page
      trackingData.utm_source = attrData.utm_source;
      trackingData.utm_content = attrData.utm_content;
      trackingData.utm_term = attrData.utm_term;
      trackingData.referrer_url = attrData.referrer_url;
      
      console.log('✅ Attribution applied to tracking data:', {
        method: trackingData.attribution_method,
        score: trackingData.attribution_score,
        source: trackingData.source,
        landing_page: trackingData.landing_page ? 'SET' : 'MISSING'  // ← Debug landing page
      });
    }

    // Enhanced tracking data with comprehensive IP information
    const enhancedTrackingData = {
      ...trackingData,
      
      // IP tracking metadata  
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP || 'unknown',
      primary_ip: extractedData.PIP || null,
      conversion_ip: extractedData.CIP || null,
      pageview_ip: extractedData.IP || null,
      
      // Enhanced dual IP detection
      dual_ip_scenario: isDualIPScenario,
      ip_addresses_detected: uniqueIPs.length,
      unique_ips: uniqueIPs
    };

    console.log('Final tracking data prepared:', {
      email: enhancedTrackingData.email,
      attribution_found: enhancedTrackingData.attribution_found,
      attribution_method: enhancedTrackingData.attribution_method,
      source: enhancedTrackingData.source,
      landing_page: enhancedTrackingData.landing_page ? 'PRESENT' : 'MISSING'  // ← Final verification
    });

    // Store conversion data
    try {
      const storageKey = `conversions:${enhancedTrackingData.timestamp}:${Math.random().toString(36).substr(2, 9)}`;
      await redis(`setex/${storageKey}/86400/${encodeURIComponent(JSON.stringify(enhancedTrackingData))}`, 2000);
      console.log('✅ Conversion data stored with key:', storageKey);
    } catch (storageError) {
      console.log('⚠️ Storage failed:', storageError.message);
    }

    // Store attribution stats for monitoring
    try {
      const statsKey = `attribution_stats_${Date.now()}`;
      const statsData = {
        email: enhancedTrackingData.email,
        timestamp: enhancedTrackingData.timestamp,
        method: enhancedTrackingData.attribution_method,
        score: enhancedTrackingData.attribution_score,
        success: enhancedTrackingData.attribution_found,
        dual_ip: isDualIPScenario,
        fields_available: enhancedTrackingData.attribution_fields_present
      };
      await redis(`setex/${statsKey}/2592000/${encodeURIComponent(JSON.stringify(statsData))}`, 1000); // 30 days
      console.log('✅ Attribution stats stored');
    } catch (statsError) {
      console.log('⚠️ Stats storage failed:', statsError.message);
    }

    // Return success response
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        success: true,
        message: isSubscriptionEvent ? 
          'Subscription tracked successfully' : 'Conversion tracked successfully',
        attribution_found: enhancedTrackingData.attribution_found,
        attribution_method: enhancedTrackingData.attribution_method,
        attribution_score: enhancedTrackingData.attribution_score || 0,
        order_id: enhancedTrackingData.order_id,
        event_type: enhancedTrackingData.event_type,
        dual_ip_detected: isDualIPScenario,
        attribution_fields_received: enhancedTrackingData.attribution_fields_present,
        
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
          landing_page_copied: !!enhancedTrackingData.landing_page,  // ← NEW: Verify fix
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
