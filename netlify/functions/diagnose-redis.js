// Create this as: netlify/functions/diagnose-redis.js
// Quick diagnostic tool to see what's actually in Redis

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  const redis = (command) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
    return fetch(url, {
      headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
    }).then(r => r.json());
  };

  try {
    console.log('🔍 REDIS DIAGNOSTIC: Scanning for attribution keys...');
    
    // Step 1: Scan for all attribution keys
    const scanResult = await redis(`scan/0/match/attribution_*/count/50`);
    console.log('Scan result:', scanResult);
    
    if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          message: 'No attribution keys found',
          scan_result: scanResult
        })
      };
    }
    
    const keys = scanResult.result[1] || [];
    console.log(`Found ${keys.length} attribution keys`);
    
    // Step 2: Analyze key patterns
    const keyPatterns = {
      main_keys: [],
      lookup_keys: [],
      unknown_keys: []
    };
    
    keys.forEach(key => {
      if (key.includes('_ip_') || key.includes('_session_') || key.includes('_fp_') || 
          key.includes('_screen_') || key.includes('_webgl_') || key.includes('_geo_')) {
        keyPatterns.lookup_keys.push(key);
      } else if (key.startsWith('attribution_') && key.match(/\d+$/)) {
        keyPatterns.main_keys.push(key);
      } else {
        keyPatterns.unknown_keys.push(key);
      }
    });
    
    // Step 3: Sample a few main keys to see their data structure
    const sampleData = {};
    const samplesToCheck = keyPatterns.main_keys.slice(0, 3);
    
    for (const key of samplesToCheck) {
      try {
        const data = await redis(`get/${key}`);
        if (data?.result) {
          const parsed = JSON.parse(data.result);
          sampleData[key] = {
            has_timestamp: !!parsed.timestamp,
            has_ip_address: !!parsed.ip_address,
            has_landing_page: !!(parsed.landing_page || parsed.url || parsed.page_url),
            has_source: !!parsed.source,
            keys_in_data: Object.keys(parsed)
          };
        }
      } catch (e) {
        sampleData[key] = { error: 'Failed to parse JSON' };
      }
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        total_keys_found: keys.length,
        key_patterns: {
          main_keys_count: keyPatterns.main_keys.length,
          lookup_keys_count: keyPatterns.lookup_keys.length,
          unknown_keys_count: keyPatterns.unknown_keys.length
        },
        sample_main_keys: keyPatterns.main_keys.slice(0, 5),
        sample_lookup_keys: keyPatterns.lookup_keys.slice(0, 5),
        sample_data_structure: sampleData,
        diagnosis: {
          likely_issue: keyPatterns.main_keys.length === 0 ? 
            'No main attribution keys found - all keys appear to be lookup keys' :
            'Main keys exist but may have different data structure than expected',
          recommended_action: keyPatterns.main_keys.length === 0 ?
            'Check if pageviews are being stored correctly by store-attribution.js' :
            'Adjust extraction filter logic to match actual key patterns'
        }
      })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Diagnostic failed', 
        message: error.message 
      })
    };
  }
};
