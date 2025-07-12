// redis-lookup.js - Direct Redis key lookup for debugging
// Path: netlify/functions/redis-lookup.js

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Max-Age': '86400',
  'Content-Type': 'application/json'
};

function createCorsResponse(statusCode, body) {
  return {
    statusCode,
    headers: CORS_HEADERS,
    body: typeof body === 'string' ? body : JSON.stringify(body)
  };
}

exports.handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return createCorsResponse(200, { message: 'CORS preflight successful' });
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return createCorsResponse(401, { error: 'Invalid API key' });
  }

  const redis = (path) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    const requestBody = event.body ? JSON.parse(event.body) : {};
    const queryParams = event.queryStringParameters || {};
    
    // Get key from body or query params
    const key = requestBody.key || queryParams.key;
    
    if (!key) {
      return createCorsResponse(400, { 
        error: 'Redis key required',
        usage: 'POST with {"key": "redis_key_name"} or GET with ?key=redis_key_name'
      });
    }

    console.log(`🔍 Redis lookup for key: ${key}`);

    // Lookup the key
    const result = await redis(`get/${key}`);
    
    if (result.result) {
      // Try to parse as JSON if possible
      let parsedData;
      try {
        parsedData = JSON.parse(decodeURIComponent(result.result));
      } catch (parseError) {
        // If parsing fails, return raw data
        parsedData = result.result;
      }
      
      return createCorsResponse(200, {
        success: true,
        key: key,
        found: true,
        data: parsedData,
        raw_data: result.result
      });
    } else {
      return createCorsResponse(404, {
        success: true,
        key: key,
        found: false,
        message: 'Key not found in Redis'
      });
    }

  } catch (error) {
    console.error('❌ Redis lookup error:', error);
    return createCorsResponse(500, {
      success: false,
      error: 'Redis lookup failed',
      message: error.message
    });
  }
};
