// File: netlify/functions/cleanup-attribution.js
// Redis Memory Optimization and Cleanup

const handler = async (event, context) => {
  // Handle CORS preflight requests
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      },
      body: ''
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      },
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  // Security check
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      },
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

  try {
    const cleanupStats = await performCleanup(redis);
    
    return {
      statusCode: 200,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        success: true,
        message: 'Cleanup completed successfully',
        stats: cleanupStats
      })
    };
  } catch (error) {
    console.error('❌ Cleanup error:', error);
    return {
      statusCode: 500,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ error: error.message })
    };
  }
};

async function performCleanup(redis) {
  const threeDaysAgo = Date.now() - (3 * 24 * 60 * 60 * 1000);
  const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
  
  console.log('🧹 Starting Redis cleanup...');
  
  let deletedKeys = 0;
  let checkedKeys = 0;
  
  // 1. Clean old attribution stats (keep only 72 hours)
  try {
    const statsKeys = await redis('keys/attribution_stats_*');
    if (statsKeys.result && statsKeys.result.length > 0) {
      console.log(`🔍 Checking ${statsKeys.result.length} attribution stats keys...`);
      
      for (const key of statsKeys.result) {
        checkedKeys++;
        const timestamp = parseInt(key.split('_').pop());
        if (timestamp && timestamp < threeDaysAgo) {
          await redis(`del/${key}`);
          deletedKeys++;
        }
      }
      console.log(`✅ Attribution stats cleanup: ${deletedKeys} keys deleted`);
    }
  } catch (error) {
    console.error('⚠️ Attribution stats cleanup failed:', error);
  }
  
  // 2. Clean old attribution data (keep only 7 days of main attribution data)
  try {
    const attributionKeys = await redis('keys/attribution_*');
    if (attributionKeys.result && attributionKeys.result.length > 0) {
      const mainKeys = attributionKeys.result.filter(key => 
        !key.startsWith('attribution_ip_') && 
        !key.startsWith('attribution_session_') &&
        !key.startsWith('attribution_fp_') &&
        !key.startsWith('attribution_webgl_') &&
        !key.startsWith('attribution_geo_') &&
        !key.startsWith('attribution_region_')
      );
      
      console.log(`🔍 Checking ${mainKeys.length} main attribution keys...`);
      let oldAttributionDeleted = 0;
      
      for (const key of mainKeys) {
        checkedKeys++;
        const timestamp = parseInt(key.split('_').pop());
        if (timestamp && timestamp < sevenDaysAgo) {
          await redis(`del/${key}`);
          deletedKeys++;
          oldAttributionDeleted++;
        }
      }
      console.log(`✅ Old attribution cleanup: ${oldAttributionDeleted} keys deleted`);
    }
  } catch (error) {
    console.error('⚠️ Attribution cleanup failed:', error);
  }
  
  // 3. Clean orphaned lookup keys (TTL should handle this, but double-check)
  try {
    const lookupKeys = await redis('keys/attribution_ip_*');
    if (lookupKeys.result && lookupKeys.result.length > 1000) {
      console.log(`⚠️ High number of IP lookup keys: ${lookupKeys.result.length}`);
      // If there are too many, delete older ones
      const sortedKeys = lookupKeys.result.sort();
      const toDelete = sortedKeys.slice(0, sortedKeys.length - 500); // Keep last 500
      
      for (const key of toDelete) {
        await redis(`del/${key}`);
        deletedKeys++;
      }
      console.log(`✅ Cleaned ${toDelete.length} old lookup keys`);
    }
  } catch (error) {
    console.error('⚠️ Lookup key cleanup failed:', error);
  }
  
  // 4. Geographic cache optimization
  try {
    const geoKeys = await redis('keys/geo_cache:*');
    if (geoKeys.result && geoKeys.result.length > 1000) {
      console.log(`🌍 Geographic cache size: ${geoKeys.result.length} keys`);
      // Implement LRU cleanup if needed
      // For now, just log the size
    }
  } catch (error) {
    console.error('⚠️ Geographic cache check failed:', error);
  }
  
  // 5. Clean old pageview data (keep only 14 days)
  try {
    const pageviewKeys = await redis('keys/pageview_*');
    if (pageviewKeys.result && pageviewKeys.result.length > 0) {
      console.log(`📄 Found ${pageviewKeys.result.length} pageview keys`);
      
      const fourteenDaysAgo = Date.now() - (14 * 24 * 60 * 60 * 1000);
      let oldPageviewsDeleted = 0;
      
      for (const key of pageviewKeys.result.slice(0, 100)) { // Process in batches
        checkedKeys++;
        const timestamp = parseInt(key.split('_').pop());
        if (timestamp && timestamp < fourteenDaysAgo) {
          await redis(`del/${key}`);
          deletedKeys++;
          oldPageviewsDeleted++;
        }
      }
      console.log(`✅ Old pageviews cleanup: ${oldPageviewsDeleted} keys deleted`);
    }
  } catch (error) {
    console.error('⚠️ Pageview cleanup failed:', error);
  }
  
  // 6. Memory usage estimation
  try {
    const memoryInfo = await redis('memory/usage');
    console.log('💾 Redis memory usage:', memoryInfo);
  } catch (error) {
    console.log('⚠️ Could not get memory usage info');
  }
  
  // 7. Get final key count
  try {
    const allKeys = await redis('keys/*');
    const totalKeysAfter = allKeys.result ? allKeys.result.length : 0;
    console.log(`🔑 Total keys after cleanup: ${totalKeysAfter}`);
  } catch (error) {
    console.log('⚠️ Could not get final key count');
  }
  
  return {
    keys_checked: checkedKeys,
    keys_deleted: deletedKeys,
    cleanup_timestamp: new Date().toISOString(),
    retention_policy: {
      attribution_stats: '72 hours',
      main_attribution: '7 days',
      pageviews: '14 days',
      geographic_cache: '24 hours (TTL)',
      lookup_keys: '24 hours (TTL)'
    }
  };
}

module.exports = { handler };
