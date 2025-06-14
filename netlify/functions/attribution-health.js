// File: netlify/functions/attribution-health.js
// Attribution Health Monitoring Endpoint

const handler = async (event, context) => {
  // Handle CORS preflight requests
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, OPTIONS'
      },
      body: ''
    };
  }

  // Only allow GET requests
  if (event.httpMethod !== 'GET') {
    return {
      statusCode: 405,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, OPTIONS'
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
        'Access-Control-Allow-Methods': 'GET, OPTIONS'
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
    console.log('🔍 Checking attribution system health...');
    
    const healthData = await checkSystemHealth(redis);
    
    return {
      statusCode: 200,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(healthData)
    };
  } catch (error) {
    console.error('❌ Health check error:', error);
    return {
      statusCode: 500,
      headers: { 
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ 
        status: 'critical',
        error: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

async function checkSystemHealth(redis) {
  const now = Date.now();
  const oneDayAgo = now - (24 * 60 * 60 * 1000);
  const oneWeekAgo = now - (7 * 24 * 60 * 60 * 1000);
  
  console.log('📊 Analyzing attribution system health...');
  
  let healthStatus = {
    status: 'healthy',
    success_rate: 0,
    geographic_correlation: {
      enabled: true,
      success_rate: 0
    },
    ipv6_metrics: {
      dual_stack_ready: true,
      pageviews_24h: 0
    },
    alerts: [],
    last_checked: new Date().toISOString()
  };

  try {
    // 1. Check attribution success rate (last 24 hours)
    const conversionKeys = await redis('keys/conversion_*');
    if (conversionKeys.result && conversionKeys.result.length > 0) {
      let totalConversions = 0;
      let attributedConversions = 0;
      
      // Sample recent conversions to check attribution status
      const recentConversions = conversionKeys.result.slice(-20); // Check last 20
      
      for (const key of recentConversions) {
        try {
          const conversionData = await redis(`get/${key}`);
          if (conversionData.result) {
            const conversion = JSON.parse(conversionData.result);
            totalConversions++;
            
            // Check if attribution was found
            if (conversion.attribution_found || conversion.attribution_method) {
              attributedConversions++;
            }
          }
        } catch (parseError) {
          // Skip invalid conversion data
          continue;
        }
      }
      
      if (totalConversions > 0) {
        healthStatus.success_rate = Math.round((attributedConversions / totalConversions) * 100);
        console.log(`📈 Attribution success rate: ${healthStatus.success_rate}% (${attributedConversions}/${totalConversions})`);
        
        // Set status based on success rate
        if (healthStatus.success_rate < 60) {
          healthStatus.status = 'critical';
          healthStatus.alerts.push('Attribution success rate below 60%');
        } else if (healthStatus.success_rate < 80) {
          healthStatus.status = 'warning';
          healthStatus.alerts.push('Attribution success rate below 80%');
        }
      }
    }

    // 2. Check geographic correlation performance
    const geoKeys = await redis('keys/attribution_geo_*');
    if (geoKeys.result && geoKeys.result.length > 0) {
      console.log(`🌍 Found ${geoKeys.result.length} geographic attribution keys`);
      
      let geoSuccesses = 0;
      const sampleGeoKeys = geoKeys.result.slice(-10); // Check last 10
      
      for (const key of sampleGeoKeys) {
        try {
          const geoData = await redis(`get/${key}`);
          if (geoData.result) {
            const geo = JSON.parse(geoData.result);
            if (geo.confidence && geo.confidence > 0.5) {
              geoSuccesses++;
            }
          }
        } catch (parseError) {
          continue;
        }
      }
      
      if (sampleGeoKeys.length > 0) {
        healthStatus.geographic_correlation.success_rate = Math.round((geoSuccesses / sampleGeoKeys.length) * 100);
        console.log(`🌍 Geographic correlation success: ${healthStatus.geographic_correlation.success_rate}%`);
      }
    } else {
      console.log('🌍 No geographic correlation keys found');
    }

    // 3. Check IPv6 readiness and activity
    const pageviewKeys = await redis('keys/pageview_*');
    if (pageviewKeys.result && pageviewKeys.result.length > 0) {
      let ipv6Pageviews = 0;
      let totalPageviews = 0;
      
      // Sample recent pageviews to check IPv6 support
      const recentPageviews = pageviewKeys.result.slice(-20);
      
      for (const key of recentPageviews) {
        try {
          const pageviewData = await redis(`get/${key}`);
          if (pageviewData.result) {
            const pageview = JSON.parse(pageviewData.result);
            totalPageviews++;
            
            // Check if this is an IPv6 address
            if (pageview.ip_address && pageview.ip_address.includes(':')) {
              ipv6Pageviews++;
            }
          }
        } catch (parseError) {
          continue;
        }
      }
      
      healthStatus.ipv6_metrics.pageviews_24h = ipv6Pageviews;
      console.log(`🌐 IPv6 pageviews in sample: ${ipv6Pageviews}/${totalPageviews}`);
      
      if (totalPageviews > 10 && ipv6Pageviews === 0) {
        healthStatus.alerts.push('No IPv6 traffic detected - dual-stack attribution untested');
      }
    }

    // 4. Check Redis key health and memory usage
    const allKeys = await redis('keys/*');
    if (allKeys.result && allKeys.result.length > 0) {
      const totalKeys = allKeys.result.length;
      console.log(`🔑 Total Redis keys: ${totalKeys}`);
      
      if (totalKeys > 10000) {
        healthStatus.status = 'warning';
        healthStatus.alerts.push(`High Redis key count: ${totalKeys}. Consider running cleanup.`);
      }
      
      // Check for attribution-specific keys
      const attributionKeys = allKeys.result.filter(key => key.startsWith('attribution_'));
      const pageviewKeys = allKeys.result.filter(key => key.startsWith('pageview_'));
      const conversionKeys = allKeys.result.filter(key => key.startsWith('conversion_'));
      
      console.log(`📊 Key breakdown - Attribution: ${attributionKeys.length}, Pageviews: ${pageviewKeys.length}, Conversions: ${conversionKeys.length}`);
    }

    // 5. Test basic Redis functionality
    const testKey = `health_check_${now}`;
    await redis(`set/${testKey}/test_value`);
    const testResult = await redis(`get/${testKey}`);
    await redis(`del/${testKey}`);
    
    if (testResult.result !== 'test_value') {
      healthStatus.status = 'critical';
      healthStatus.alerts.push('Redis read/write test failed');
    }

    // 6. Final status determination
    if (healthStatus.alerts.length === 0 && healthStatus.success_rate >= 80) {
      healthStatus.status = 'healthy';
    } else if (healthStatus.alerts.length > 0 && healthStatus.status === 'healthy') {
      healthStatus.status = 'warning';
    }

    console.log(`✅ Health check complete - Status: ${healthStatus.status.toUpperCase()}`);
    return healthStatus;

  } catch (error) {
    console.error('⚠️ Health check error:', error);
    return {
      status: 'critical',
      error: error.message,
      success_rate: 0,
      geographic_correlation: { enabled: false, success_rate: 0 },
      ipv6_metrics: { dual_stack_ready: false, pageviews_24h: 0 },
      alerts: ['Health check system failure'],
      last_checked: new Date().toISOString()
    };
  }
}

module.exports = { handler };
