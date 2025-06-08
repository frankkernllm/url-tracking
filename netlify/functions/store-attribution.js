// File: netlify/functions/store-attribution.js
// Redis-powered attribution storage with API Key Security

const handler = async (event, context) => {
  // Handle CORS preflight
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

  // 🔒 Security Check - Verify API Key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!validApiKey) {
    console.error('❌ No API key configured in environment');
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Server configuration error' })
    };
  }

  if (!apiKey || apiKey !== validApiKey) {
    console.log('🚫 Unauthorized access attempt');
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  console.log('✅ API key validated');

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!redisUrl || !redisToken) {
    console.error('❌ Missing Redis credentials');
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Redis not configured' })
    };
  }

  // Redis helper function
  const redis = async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };

  if (event.httpMethod === 'POST') {
    try {
      const attributionData = JSON.parse(event.body);
      
      console.log('📊 Storing attribution data:', JSON.stringify(attributionData, null, 2));
      
      // Store attribution data with multiple lookup keys for flexibility
      const baseKey = `attribution:${attributionData.ip_address}:${Date.now()}`;
      const ipKey = `attribution_ip:${attributionData.ip_address}`;
      const sessionKey = `attribution_session:${attributionData.session_id}`;
      
      // Store the full attribution data
      await redis(`set/${baseKey}/${encodeURIComponent(JSON.stringify(attributionData))}`);
      
      // Store lookup keys that expire after 24 hours (86400 seconds)
      await redis(`setex/${ipKey}/86400/${baseKey}`);
      await redis(`setex/${sessionKey}/86400/${baseKey}`);
      
      console.log('✅ Attribution stored with 3 lookup keys');
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          success: true, 
          message: 'Attribution data stored successfully',
          keys_created: 3
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
      const { ip, timestamp } = event.queryStringParameters || {};
      
      if (!ip) {
        return {
          statusCode: 400,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ error: 'IP address required', found: false })
        };
      }
      
      console.log(`🔍 Looking up attribution for IP: ${ip}`);
      
      // Try to find attribution data by IP
      const ipLookupKey = `attribution_ip:${ip}`;
      const lookupResult = await redis(`get/${ipLookupKey}`);
      
      if (!lookupResult.result) {
        console.log('⚠️ No attribution found for IP');
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ found: false, message: 'No attribution data found' })
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
          body: JSON.stringify({ found: false, message: 'Attribution data expired' })
        };
      }
      
      const attributionData = JSON.parse(attributionResult.result);
      console.log('✅ Attribution data found and returned');
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          found: true, 
          data: attributionData,
          lookup_method: 'ip_address'
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
