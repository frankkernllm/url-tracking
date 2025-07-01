// File: netlify/functions/attribution-data-scanner.js
// Quick scanner to see what attribution data exists in Redis

const handler = async (event, context) => {
  console.log('🔍 Scanning for available attribution data...');
  
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
    
    const redis = async (command) => {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { Authorization: `Bearer ${redisToken}` }
      });
      return response.json();
    };

    console.log('📊 Scanning for attribution patterns...');
    
    const patterns = [
      'attribution_session_*',
      'attribution_ip_*', 
      'attribution_fp_*',
      'attribution_screen_*',
      'attribution_webgl_*'
    ];
    
    const results = {};
    let totalFound = 0;
    
    for (const pattern of patterns) {
      try {
        let cursor = '0';
        let keys = [];
        let iterations = 0;
        
        do {
          const result = await redis(`scan/${cursor}/match/${pattern}/count/100`);
          if (result.result && result.result[1]) {
            cursor = result.result[0];
            const batchKeys = result.result[1];
            keys = keys.concat(batchKeys);
            iterations++;
            
            if (iterations > 10) break; // Limit for quick scan
          } else {
            break;
          }
        } while (cursor !== '0');
        
        results[pattern] = {
          count: keys.length,
          samples: keys.slice(0, 5)
        };
        totalFound += keys.length;
        
        console.log(`✅ ${pattern}: Found ${keys.length} keys`);
        
      } catch (error) {
        console.log(`❌ ${pattern} scan failed:`, error.message);
        results[pattern] = { count: 0, error: error.message };
      }
    }
    
    // Sample some attribution data to see structure
    const sampleData = [];
    for (const pattern in results) {
      if (results[pattern].samples && results[pattern].samples.length > 0) {
        const sampleKey = results[pattern].samples[0];
        try {
          const dataResult = await redis(`get/${sampleKey}`);
          if (dataResult.result) {
            const parsed = JSON.parse(dataResult.result);
            sampleData.push({
              pattern: pattern,
              key: sampleKey,
              data: {
                source: parsed.source,
                landing_page: parsed.landing_page?.substring(0, 50),
                timestamp: parsed.timestamp,
                utm_source: parsed.utm_source
              }
            });
          }
        } catch (error) {
          console.log(`Error sampling ${sampleKey}:`, error.message);
        }
      }
    }

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        message: 'Attribution data scan completed',
        results: {
          total_attribution_keys: totalFound,
          by_pattern: results,
          sample_data: sampleData,
          scan_date: new Date().toISOString()
        }
      })
    };

  } catch (error) {
    console.error('❌ Scanner error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Scan failed', 
        details: error.message 
      })
    };
  }
};

module.exports = { handler };
