// File: netlify/functions/store-attribution.js
// FINAL FIX: IPv6-safe key pattern using URL encoding

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
      }
    };
  }

  // Security Check
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };

  // Extract real visitor IP from request headers
  function getVisitorIP(event) {
    const forwarded = event.headers['x-forwarded-for'];
    const realIP = event.headers['x-real-ip'];
    const cfIP = event.headers['cf-connecting-ip'];
    const clientIP = event.headers['x-client-ip'];
    
    if (forwarded) {
      return forwarded.split(',')[0].trim();
    }
    if (cfIP) return cfIP;
    if (realIP) return realIP;
    if (clientIP) return clientIP;
    
    return 'unknown';
  }

  // IPv6-safe key encoding
  function encodeIPForKey(ip) {
    // Replace colons with underscores for IPv6 addresses
    return ip.replace(/:/g, '_');
  }

  if (event.httpMethod === 'POST') {
    try {
      const attributionData = JSON.parse(event.body);
      
      const visitorIP = getVisitorIP(event);
      attributionData.ip_address = visitorIP;
      
      console.log('📊 Storing attribution data:', {
        session_id: attributionData.session_id,
        ip_address: visitorIP,
        source: attributionData.source,
        landing_page: attributionData.landing_page
      });
      
      const timestamp = Date.now();
      const encodedIP = encodeIPForKey(visitorIP);
      
      // FIXED: IPv6-safe key pattern
      const baseKey = `attribution_${encodedIP}_${timestamp}`;
      
      console.log('🔑 Creating base key:', baseKey);
      
      // Store the full attribution data
      await redis(`set/${baseKey}/${encodeURIComponent(JSON.stringify(attributionData))}`);
      console.log('✅ Stored attribution with key:', baseKey);
      
      // Create lookup keys with longer expiration (24 hours = 86400 seconds)
      const ipKey = `attribution_ip_${encodedIP}`;
      const sessionKey = `attribution_session_${attributionData.session_id}`;
      
      await redis(`setex/${ipKey}/86400/${baseKey}`);
      await redis(`setex/${sessionKey}/86400/${baseKey}`);
      
      console.log('✅ Created lookup keys:', { ipKey, sessionKey });
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          success: true, 
          message: 'Attribution data stored successfully',
          visitor_ip: visitorIP,
          keys_created: 3,
          base_key: baseKey,
          encoded_ip: encodedIP
        })
      };
      
    } catch (error) {
      console.error('❌ Attribution storage error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'GET') {
    try {
      const { ip, session_id, timestamp } = event.queryStringParameters || {};
      
      console.log('🔍 GET request params:', { ip, session_id, timestamp });
      
      if (!ip && !session_id) {
        return {
          statusCode: 400,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            error: 'Either ip or session_id parameter is required',
            found: false 
          })
        };
      }
      
      let lookupResult = null;
      let lookupMethod = 'none';
      
      // Try different lookup methods
      if (ip) {
        const encodedIP = encodeIPForKey(ip);
        console.log(`🔍 Looking up attribution for encoded IP: ${encodedIP}`);
        const ipLookupKey = `attribution_ip_${encodedIP}`;
        lookupResult = await redis(`get/${ipLookupKey}`);
        if (lookupResult.result) {
          lookupMethod = 'ip_address';
          console.log('✅ Found via IP lookup');
        }
      }
      
      if (!lookupResult?.result && session_id) {
        console.log(`🔍 Looking up attribution for session: ${session_id}`);
        const sessionLookupKey = `attribution_session_${session_id}`;
        lookupResult = await redis(`get/${sessionLookupKey}`);
        if (lookupResult.result) {
          lookupMethod = 'session_id';
          console.log('✅ Found via session lookup');
        }
      }
      
      if (!lookupResult?.result) {
        console.log('⚠️ No attribution found');
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            found: false, 
            message: 'No attribution data found',
            searched_for: { ip, session_id }
          })
        };
      }
      
      // Get the actual attribution data
      const attributionKey = lookupResult.result;
      const attributionResult = await redis(`get/${attributionKey}`);
      
      if (!attributionResult.result) {
        console.log('⚠️ Attribution key found but data missing');
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            found: false, 
            message: 'Attribution data expired',
            key_found: attributionKey
          })
        };
      }
      
      const attributionData = JSON.parse(attributionResult.result);
      console.log('✅ Attribution data found via:', lookupMethod);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          found: true, 
          data: attributionData,
          lookup_method: lookupMethod,
          redis_key: attributionKey
        })
      };
      
    } catch (error) {
      console.error('❌ Attribution lookup error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message, found: false })
      };
    }
  }
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

module.exports = { handler };
