// Test Specific Missing Keys
// Path: netlify/functions/test-specific-keys.js

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    
    console.log('🔍 Testing specific keys that query-pageviews-enhanced referenced...');
    
    // The exact keys from your original query results
    const specificKeys = [
      'attribution_42.61.210.120_1753484654828',
      'attribution_42.61.210.120_1753484618503', 
      'attribution_42.61.210.120_1753463169445'
    ];
    
    const keyTests = [];
    
    for (const key of specificKeys) {
      try {
        console.log(`🔍 Testing key: ${key}`);
        const result = await redis(`get/${key}`, 2000);
        
        let keyData = null;
        if (result?.result) {
          try {
            keyData = JSON.parse(result.result);
          } catch {
            try {
              keyData = JSON.parse(decodeURIComponent(result.result));
            } catch {
              keyData = null;
            }
          }
        }
        
        keyTests.push({
          key: key,
          exists: !!result?.result,
          data_valid: !!keyData,
          sample_data: keyData ? {
            timestamp: keyData.timestamp,
            session_id: keyData.session_id,
            source: keyData.source,
            landing_page: keyData.landing_page,
            ip_address: keyData.ip_address
          } : null
        });
        
        console.log(`🔍 Key ${key}: ${result?.result ? 'EXISTS' : 'MISSING'}`);
        
      } catch (error) {
        keyTests.push({
          key: key,
          exists: false,
          error: error.message
        });
        console.log(`🔍 Key ${key}: ERROR - ${error.message}`);
      }
    }
    
    // Test fallback attribution methods that query-pageviews-enhanced might have used
    console.log('🔍 Testing fallback attribution methods...');
    
    const fallbackTests = {};
    
    // Test session lookup
    try {
      const sessionKey = 'attribution_session_1753484654185-fry3jl2k1';
      const sessionResult = await redis(`get/${sessionKey}`, 2000);
      fallbackTests.session_lookup = {
        key: sessionKey,
        exists: !!sessionResult?.result,
        data: sessionResult?.result || null
      };
      console.log(`🔍 Session key ${sessionKey}: ${sessionResult?.result ? 'EXISTS' : 'MISSING'}`);
    } catch (error) {
      fallbackTests.session_lookup = { error: error.message };
    }
    
    // Test IP lookup
    try {
      const ipKey = 'attribution_ip_42.61.210.120';
      const ipResult = await redis(`get/${ipKey}`, 2000);
      fallbackTests.ip_lookup = {
        key: ipKey,
        exists: !!ipResult?.result,
        data: ipResult?.result || null
      };
      console.log(`🔍 IP key ${ipKey}: ${ipResult?.result ? 'EXISTS' : 'MISSING'}`);
    } catch (error) {
      fallbackTests.ip_lookup = { error: error.message };
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        specific_key_tests: keyTests,
        fallback_method_tests: fallbackTests,
        analysis: {
          target_keys_missing: keyTests.filter(t => !t.exists).length,
          target_keys_existing: keyTests.filter(t => t.exists).length,
          theory: keyTests.some(t => t.exists) 
            ? "Some target keys exist - extraction should have found them"
            : "All target keys missing - explains why extraction didn't find them",
          next_steps: keyTests.some(t => t.exists)
            ? "Debug why extraction didn't process existing target keys"
            : "Investigate how query-pageviews-enhanced found non-existent keys"
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Specific key test failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Specific key test failed', 
        message: error.message 
      })
    };
  }
};

function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
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
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
