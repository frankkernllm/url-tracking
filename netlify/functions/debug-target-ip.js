// Debug Target IP Index - Check if enhanced index exists
// Path: netlify/functions/debug-target-ip.js

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
    
    console.log('🔍 Checking target IP 42.61.210.120 in both raw keys and enhanced indexes...');
    
    // Check 1: Enhanced IP Index (where query-pageviews-enhanced looks)
    const enhancedIPKey = 'pageview_index_ip:42_61_210_120';
    console.log(`🔍 Checking enhanced IP index: ${enhancedIPKey}`);
    
    const enhancedResult = await redis(`get/${enhancedIPKey}`, 3000);
    let enhancedIndexData = null;
    
    if (enhancedResult?.result) {
      enhancedIndexData = JSON.parse(decodeURIComponent(enhancedResult.result));
      console.log(`✅ Enhanced IP index FOUND with ${enhancedIndexData.pageviews?.length || 0} pageviews`);
    } else {
      console.log(`❌ Enhanced IP index NOT FOUND`);
    }
    
    // Check 2: Scan for raw attribution keys (where extract-attribution-data looks)
    console.log(`🔍 Scanning for raw attribution keys: attribution_42.61.210.120_*`);
    
    const scanResult = await redis(`scan/0/match/attribution_42.61.210.120_*/count/20`);
    let rawKeysFound = [];
    
    if (scanResult?.result && scanResult.result[1]?.length > 0) {
      rawKeysFound = scanResult.result[1];
      console.log(`✅ Raw attribution keys FOUND: ${rawKeysFound.length} keys`);
      console.log(`📋 Raw keys:`, rawKeysFound);
    } else {
      console.log(`❌ Raw attribution keys NOT FOUND`);
    }
    
    // Check 3: Test individual raw keys from enhanced index
    let rawKeyTests = [];
    if (enhancedIndexData?.pageviews) {
      console.log(`🔍 Testing raw keys referenced in enhanced index...`);
      
      const referencedKeys = enhancedIndexData.pageviews
        .map(pv => pv.redis_key)
        .filter(Boolean)
        .slice(0, 3); // Test first 3
      
      for (const key of referencedKeys) {
        try {
          const keyResult = await redis(`get/${key}`, 1000);
          rawKeyTests.push({
            key: key,
            exists: !!keyResult?.result,
            data_valid: keyResult?.result ? true : false
          });
          console.log(`🔍 Raw key ${key}: ${keyResult?.result ? 'EXISTS' : 'MISSING'}`);
        } catch (error) {
          rawKeyTests.push({
            key: key,
            exists: false,
            error: error.message
          });
        }
      }
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        target_ip: '42.61.210.120',
        debug_results: {
          enhanced_ip_index: {
            key: enhancedIPKey,
            exists: !!enhancedIndexData,
            pageview_count: enhancedIndexData?.pageviews?.length || 0,
            sample_pageviews: enhancedIndexData?.pageviews?.slice(0, 2).map(pv => ({
              timestamp: pv.timestamp,
              session_id: pv.session_id,
              source: pv.source,
              redis_key: pv.redis_key
            })) || []
          },
          raw_attribution_keys: {
            scan_pattern: 'attribution_42.61.210.120_*',
            keys_found: rawKeysFound.length,
            keys: rawKeysFound
          },
          raw_key_tests: rawKeyTests
        },
        conclusion: {
          enhanced_index_exists: !!enhancedIndexData,
          raw_keys_exist: rawKeysFound.length > 0,
          data_mismatch: !!enhancedIndexData && rawKeysFound.length === 0,
          explanation: !!enhancedIndexData && rawKeysFound.length === 0 
            ? "Enhanced index contains data but raw keys are missing - explains why query-pageviews-enhanced works but extract-attribution-data doesn't find the target IP"
            : "Need further investigation"
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Debug failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Debug failed', 
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
