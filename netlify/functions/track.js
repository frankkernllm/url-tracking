// File: netlify/functions/track.js
// Clean version with no syntax errors

const handler = async (event, context) => {
  console.log('Track function started');

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

    // Extract data safely
    const extractedData = {
      email: data.email || data.customer?.email || 'unknown',
      order_total: parseFloat(data.order_total) || 0,
      order_id: data.order_id || 'unknown',
      
      // IP addresses
      PIP: data.custom_ipv6 || data.custom_ipv4,
      CIP: data.ip,
      IP: data.checkoutview?.pageviewcheckout?.pageview?.ip,
      
      // Attribution parameters
      SSID: data.ssid,
      dsig: data.dsig,
      SVV: data.svvv || data.SVV,
      gsig: data.gsig,
      
      // UTM parameters
      utm_source: data.utm_source || data.checkoutview?.utm_source,
      utm_campaign: data.utm_campaign || data.checkoutview?.utm_campaign,
      utm_medium: data.utm_medium || data.checkoutview?.utm_medium,
      
      timestamp: new Date().toISOString()
    };

    console.log('Extracted data:', {
      email: extractedData.email,
      order_total: extractedData.order_total,
      has_SSID: !!extractedData.SSID,
      has_PIP: !!extractedData.PIP,
      has_CIP: !!extractedData.CIP
    });

    // Simple attribution lookup
    let attributionResult = null;

    try {
      // Try session ID lookup first
      if (extractedData.SSID) {
        console.log('Trying session ID lookup:', extractedData.SSID);
        const sessionKey = `attribution_session_${extractedData.SSID}`;
        const sessionResult = await redis(`get/${sessionKey}`, 2000);
        
        if (sessionResult.result) {
          const attributionDataResult = await redis(`get/${sessionResult.result}`, 2000);
          if (attributionDataResult.result) {
            attributionResult = {
              data: JSON.parse(attributionDataResult.result),
              method: 'session_id',
              score: 300
            };
            console.log('Session ID attribution found');
          }
        }
      }

      // Try IP lookup if session failed
      if (!attributionResult && (extractedData.PIP || extractedData.CIP)) {
        const testIP = extractedData.PIP || extractedData.CIP;
        console.log('Trying IP lookup:', testIP);
        
        const encodedIP = encodeIPForKey(testIP);
        const ipKey = `attribution_ip_${encodedIP}`;
        
        const ipResult = await redis(`get/${ipKey}`, 2000);
        if (ipResult.result) {
          const attributionDataResult = await redis(`get/${ipResult.result}`, 2000);
          if (attributionDataResult.result) {
            attributionResult = {
              data: JSON.parse(attributionDataResult.result),
              method: 'ip_address',
              score: 280
            };
            console.log('IP attribution found');
          }
        }
      }

    } catch (attributionError) {
      console.log('Attribution lookup failed:', attributionError.message);
    }

    // Build tracking data
    const trackingData = {
      timestamp: extractedData.timestamp,
      event_type: 'purchase',
      
      source: attributionResult?.data?.source || extractedData.utm_source || 'direct',
      campaign: attributionResult?.data?.utm_campaign || extractedData.utm_campaign || 'none',
      medium: attributionResult?.data?.utm_medium || extractedData.utm_medium || 'none',
      
      email: extractedData.email,
      order_id: extractedData.order_id,
      order_total: extractedData.order_total,
      
      attribution_found: !!attributionResult,
      attribution_method: attributionResult?.method || 'none',
      attribution_score: attributionResult?.score || 0,
      
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP || 'unknown'
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
        message: 'Conversion tracked successfully',
        attribution_found: trackingData.attribution_found,
        attribution_method: trackingData.attribution_method,
        order_id: trackingData.order_id
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
