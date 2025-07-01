// File: netlify/functions/attribution-recovery-june-analysis.js
// SIMPLE DEBUG VERSION - Let's see what's actually in Redis

const handler = async (event, context) => {
  console.log('🔍 SIMPLE DEBUG: Starting Redis investigation...');
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;
  
  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  try {
    const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
    const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    console.log('📋 Redis URL configured:', !!redisUrl);
    console.log('📋 Redis Token configured:', !!redisToken);
    
    const redis = async (command) => {
      console.log(`🔍 Executing Redis command: ${command}`);
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      const result = await response.json();
      console.log(`📋 Redis response:`, result);
      return result;
    };

    // Test 1: Basic Redis connection
    console.log('🧪 TEST 1: Basic Redis ping');
    try {
      const pingResult = await redis('ping');
      console.log('✅ Redis ping successful:', pingResult);
    } catch (pingError) {
      console.log('❌ Redis ping failed:', pingError);
      return {
        statusCode: 500,
        headers,
        body: JSON.stringify({ 
          error: 'Redis connection failed',
          details: pingError.message
        })
      };
    }

    // Test 2: Try to find ANY keys with common patterns
    console.log('🧪 TEST 2: Searching for ANY keys');
    const testPatterns = [
      'keys/*',           // All keys
      'keys/conv*',       // Any conversion-related
      'keys/*2025*',      // Any 2025 keys
      'keys/attribution*' // Any attribution keys
    ];
    
    let totalKeysFound = 0;
    let sampleKeys = [];
    
    for (const pattern of testPatterns) {
      try {
        console.log(`🔍 Testing pattern: ${pattern}`);
        const result = await redis(pattern);
        const keys = result.result || [];
        totalKeysFound += keys.length;
        
        console.log(`📋 Pattern ${pattern}: Found ${keys.length} keys`);
        if (keys.length > 0) {
          console.log(`📋 Sample keys: ${keys.slice(0, 3).join(', ')}`);
          sampleKeys = sampleKeys.concat(keys.slice(0, 5));
        }
      } catch (error) {
        console.log(`⚠️ Pattern ${pattern} failed:`, error.message);
      }
    }

    console.log(`📊 TOTAL KEYS FOUND: ${totalKeysFound}`);

    // Test 3: If we found keys, examine them
    let conversionSamples = [];
    if (sampleKeys.length > 0) {
      console.log('🧪 TEST 3: Examining sample keys');
      
      for (const key of sampleKeys.slice(0, 3)) {
        try {
          console.log(`🔍 Examining key: ${key}`);
          const result = await redis(`get/${key}`);
          const data = result.result;
          
          if (data) {
            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
            conversionSamples.push({
              key: key,
              has_email: !!parsed.email,
              has_timestamp: !!parsed.timestamp,
              timestamp: parsed.timestamp,
              structure: Object.keys(parsed).slice(0, 10)
            });
            console.log(`📋 Key ${key}: email=${!!parsed.email}, timestamp=${parsed.timestamp}`);
          }
        } catch (error) {
          console.log(`⚠️ Failed to examine key ${key}:`, error.message);
        }
      }
    }

    // Test 4: Try SCAN method (like analytics.js)
    console.log('🧪 TEST 4: Trying SCAN method like analytics.js');
    let scanKeys = [];
    try {
      let cursor = '0';
      let iterations = 0;
      
      do {
        const result = await redis(`scan/${cursor}/match/*/count/100`);
        if (result.result && result.result[1]) {
          cursor = result.result[0];
          const keys = result.result[1];
          scanKeys = scanKeys.concat(keys);
          iterations++;
          
          console.log(`📋 SCAN iteration ${iterations}: Found ${keys.length} keys, cursor=${cursor}`);
          
          if (iterations >= 3) break; // Limit for debug
        } else {
          break;
        }
      } while (cursor !== '0');
      
      console.log(`📊 SCAN method found ${scanKeys.length} total keys`);
      
      // Look for conversion-like keys
      const conversionLikeKeys = scanKeys.filter(key => 
        key.includes('conversion') || 
        key.includes('conv') || 
        key.includes('2025-06')
      );
      
      console.log(`📊 Conversion-like keys: ${conversionLikeKeys.length}`);
      if (conversionLikeKeys.length > 0) {
        console.log(`📋 Conversion keys found: ${conversionLikeKeys.slice(0, 5).join(', ')}`);
      }
      
    } catch (scanError) {
      console.log('❌ SCAN method failed:', scanError.message);
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Redis debug completed',
        debug_results: {
          redis_connection: 'OK',
          total_keys_found: totalKeysFound,
          sample_keys: sampleKeys.slice(0, 10),
          conversion_samples: conversionSamples,
          scan_keys_found: scanKeys.length,
          redis_url_configured: !!redisUrl,
          redis_token_configured: !!redisToken
        }
      })
    };

  } catch (error) {
    console.error('❌ Debug error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Debug failed', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
