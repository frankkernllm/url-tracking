// Create: netlify/functions/debug-ip-index.js
// Quick diagnostic to check IP index structure

exports.handler = async (event, context) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
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
    const targetIP = '1.127.108.160';
    const encodedIP = targetIP.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    console.log(`🔍 Debugging IP index for: ${targetIP}`);
    console.log(`🔑 Looking for key: ${ipIndexKey}`);
    
    // Step 1: Check if the IP index key exists
    const indexData = await redis(`get/${ipIndexKey}`);
    
    if (!indexData?.result) {
      // Step 2: If not found, scan for similar keys
      const scanResult = await redis(`scan/0/match/pageview_index_ip:*/count/100`);
      
      let foundKeys = [];
      if (scanResult?.result && Array.isArray(scanResult.result) && scanResult.result.length > 1) {
        foundKeys = scanResult.result[1] || [];
      }
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          target_ip: targetIP,
          encoded_ip: encodedIP,
          expected_key: ipIndexKey,
          key_found: false,
          available_ip_index_keys: foundKeys.slice(0, 10), // Show first 10
          total_ip_index_keys: foundKeys.length,
          diagnosis: 'IP index key not found - check key format or IP encoding'
        })
      };
    }
    
    // Step 3: If found, examine the structure
    const parsed = JSON.parse(decodeURIComponent(indexData.result));
    
    // Look for session ID in the pageviews
    const targetSession = '1750140065932-s74ts5cbh';
    const sessionMatches = parsed.pageviews?.filter(pv => pv.session_id === targetSession) || [];
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        target_ip: targetIP,
        encoded_ip: encodedIP,
        key_found: true,
        ip_index_key: ipIndexKey,
        index_structure: {
          pageview_count: parsed.pageview_count,
          latest_timestamp: parsed.latest_timestamp,
          created_at: parsed.created_at,
          sample_pageviews: parsed.pageviews?.slice(0, 3) || []
        },
        session_search: {
          target_session: targetSession,
          session_matches_found: sessionMatches.length,
          session_matches: sessionMatches
        },
        diagnosis: sessionMatches.length > 0 ? 
          'IP index found with session matches - enhanced query should work' :
          'IP index found but no session matches - check session ID or time window'
      })
    };
    
  } catch (error) {
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
