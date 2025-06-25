// File: netlify/functions/analytics.js
// ENHANCED: Attribution Stats & Health Monitoring (Tasks 4.3 & 5.2)

const handler = async (event, context) => {
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

  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
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

  // TASK 5.2: Attribution Health Check Endpoint
  if (event.httpMethod === 'GET' && event.path === '/attribution-health') {
    try {
      console.log('🩺 Attribution health check requested');
      const healthMetrics = await calculateAttributionHealth(redis);
      
      // Alert if attribution success rate drops below 70%
      if (healthMetrics.successRate < 70) {
        console.warn(`🚨 ALERT: Attribution success rate dropped to ${healthMetrics.successRate}%`);
        // Could integrate with monitoring service here (e.g., send to Slack, email, etc.)
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(healthMetrics)
      };
    } catch (error) {
      console.error('❌ Attribution health check error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message, status: 'error' })
      };
    }
  }

  if (event.httpMethod === 'GET') {
    try {
      const { start_date, end_date, source, campaign, include_attribution_stats } = event.queryStringParameters || {};
      
      console.log(`📊 Analytics query: start=${start_date}, end=${end_date}, source=${source}, campaign=${campaign}, include_stats=${include_attribution_stats}`);
      
      // 🔧 CRITICAL FIX: Use SCAN with proper cursor iteration for IPv6 support
      let attributionKeys = [];
      let conversionKeys = [];
      
      try {
        console.log('🔍 Finding ALL attribution keys including IPv6...');
        
        // Method 1: Get the first batch (mainly IPv4) using the working SCAN
        try {
          const scanResult = await redis('scan/0/match/attribution_*/count/1000');
          if (scanResult.result && scanResult.result[1]) {
            attributionKeys = scanResult.result[1].filter(key => 
              key && 
              !key.startsWith('attribution_ip_') && 
              !key.startsWith('attribution_session_') &&
              !key.startsWith('attribution_fp_') &&
              !key.startsWith('attribution_webgl_') &&
              !key.startsWith('attribution_geo_') &&
              !key.startsWith('attribution_region_')
            );
            console.log(`✅ Initial SCAN found ${attributionKeys.length} attribution keys`);
          }
        } catch (scanError) {
          console.log('⚠️ Initial SCAN failed:', scanError);
        }
        
        // Method 2: Specifically scan for IPv6 patterns (they start with 2xxx or similar)
        console.log('🔍 Scanning specifically for IPv6 addresses...');
        const ipv6Prefixes = ['2001', '2002', '2003', '2400', '2401', '2402', '2403', '2404', '2405', '2406', '2407', '2409', '2600', '2601', '2602', '2603', '2604', '2605', '2606', '2607', '2620', '2800', '2a00', '2a01', '2a02', '2a03'];
        
        for (const prefix of ipv6Prefixes) {
          try {
            const ipv6Result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);
            if (ipv6Result.result && ipv6Result.result[1] && ipv6Result.result[1].length > 0) {
              const ipv6Keys = ipv6Result.result[1].filter(key => 
                key && 
                !key.startsWith('attribution_ip_') && 
                !key.startsWith('attribution_session_') &&
                !key.startsWith('attribution_fp_') &&
                !key.startsWith('attribution_webgl_') &&
                !key.startsWith('attribution_geo_') &&
                !key.startsWith('attribution_region_')
              );
              
              if (ipv6Keys.length > 0) {
                // Add to existing keys, avoiding duplicates
                const keySet = new Set(attributionKeys);
                ipv6Keys.forEach(key => keySet.add(key));
                attributionKeys = Array.from(keySet);
                console.log(`✅ Found ${ipv6Keys.length} IPv6 keys with prefix ${prefix}`);
              }
            }
          } catch (prefixError) {
            console.log(`⚠️ Failed to scan IPv6 prefix ${prefix}:`, prefixError);
          }
        }
        
        // Method 3: If the first scan found exactly 1000 keys, try to get the next batch
        if (attributionKeys.length === 1000) {
          console.log('🔍 First scan returned exactly 1000 keys, trying to get more batches...');
          try {
            // Try with a different cursor value
            const scanResult2 = await redis('scan/1000/match/attribution_*/count/1000');
            if (scanResult2.result && scanResult2.result[1] && scanResult2.result[1].length > 0) {
              const moreKeys = scanResult2.result[1].filter(key => 
                key && 
                !key.startsWith('attribution_ip_') && 
                !key.startsWith('attribution_session_') &&
                !key.startsWith('attribution_fp_') &&
                !key.startsWith('attribution_webgl_') &&
                !key.startsWith('attribution_geo_') &&
                !key.startsWith('attribution_region_')
              );
              
              const keySet = new Set(attributionKeys);
              moreKeys.forEach(key => keySet.add(key));
              attributionKeys = Array.from(keySet);
              console.log(`✅ Second batch found ${moreKeys.length} additional keys, total: ${attributionKeys.length}`);
            }
          } catch (err) {
            console.log('⚠️ Second batch scan failed:', err);
          }
        }
        
        // Debug: Check IPv4 vs IPv6 distribution
        const ipv4Keys = attributionKeys.filter(key => {
          const parts = key.split('_');
          // IPv4 pattern: attribution_XXX_XXX_XXX_XXX_timestamp (6 parts total)
          return parts.length === 6 && /^\d+$/.test(parts[1]) && /^\d+$/.test(parts[2]);
        });
        const ipv6Keys = attributionKeys.filter(key => {
          const parts = key.split('_');
          // IPv6 has more parts due to more segments
          return parts.length > 6;
        });
        console.log(`📊 Attribution keys breakdown - IPv4: ${ipv4Keys.length}, IPv6: ${ipv6Keys.length}`);
        if (ipv6Keys.length > 0) {
          console.log(`🌐 Sample IPv6 keys:`, ipv6Keys.slice(0, 3));
        }
        
        // CRITICAL: If no keys found at all, this is a major issue
        if (attributionKeys.length === 0) {
          console.error('❌ CRITICAL: No attribution keys found! Trying emergency fallback...');
          
          // Emergency fallback - try the exact same pattern that was working before
          try {
            const scanResult = await redis('scan/0/match/attribution_*/count/1000');
            if (scanResult.result && scanResult.result[1]) {
              attributionKeys = scanResult.result[1];
              console.log(`🚨 Emergency fallback found ${attributionKeys.length} keys`);
            }
          } catch (e) {
            console.error('❌ Emergency fallback also failed:', e);
          }
        }
        
        // Get conversion keys (usually fewer, so single operation is OK)
        try {
          const conversionsResult = await redis('keys/conversions:*');
          conversionKeys = conversionsResult.result || [];
          console.log(`🔍 Found ${conversionKeys.length} conversion keys`);
        } catch (error) {
          console.log('⚠️ Failed to get conversion keys:', error);
          conversionKeys = [];
        }
        
      } catch (redisError) {
        console.error('❌ Redis operation failed:', redisError);
        attributionKeys = [];
        conversionKeys = [];
      }
      
      console.log(`📊 Final count: ${attributionKeys.length} attribution keys and ${conversionKeys.length} conversion keys`);
      
      // Fetch attribution data (handle large datasets in batches)
      let allPageViews = [];
      if (attributionKeys.length > 0) {
        try {
          console.log('📦 Fetching attribution data...');
          
          // If there are too many keys, process in batches to avoid URL length limits
          if (attributionKeys.length > 5000) {
            console.log(`⚠️ Large dataset: ${attributionKeys.length} keys. Processing in batches...`);
            
            const batchSize = 1000;
            for (let i = 0; i < attributionKeys.length; i += batchSize) {
              const batch = attributionKeys.slice(i, i + batchSize);
              try {
                const batchData = await redis(`mget/${batch.join('/')}`);
                const parsedBatch = (batchData.result || [])
                  .filter(item => item)
                  .map(item => {
                    try {
                      return JSON.parse(item);
                    } catch (parseError) {
                      return null;
                    }
                  })
                  .filter(item => item);
                
                allPageViews = allPageViews.concat(parsedBatch);
                console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(attributionKeys.length/batchSize)}: processed ${parsedBatch.length} records`);
              } catch (batchError) {
                console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, batchError);
              }
            }
            
            // Add event_type and sort
            allPageViews = allPageViews
              .map(item => ({ ...item, event_type: 'page_view' }))
              .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
            
          } else {
            // Original code for smaller datasets
            const attributionData = await redis(`mget/${attributionKeys.join('/')}`);
            allPageViews = (attributionData.result || [])
              .filter(item => item)
              .map(item => {
                try {
                  return JSON.parse(item);
                } catch (parseError) {
                  console.log('⚠️ Failed to parse attribution item');
                  return null;
                }
              })
              .filter(item => item)
              .map(item => ({ ...item, event_type: 'page_view' }))
              .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
          }
          
          console.log(`📊 Successfully parsed ${allPageViews.length} page views`);
          
          // Debug: Check IPv4 vs IPv6 in actual data
          const ipv4Data = allPageViews.filter(pv => pv.ip_address && !pv.ip_address.includes(':'));
          const ipv6Data = allPageViews.filter(pv => pv.ip_address && pv.ip_address.includes(':'));
          console.log(`📊 Page view data - IPv4: ${ipv4Data.length}, IPv6: ${ipv6Data.length}`);
          if (ipv6Data.length > 0) {
            console.log(`🌐 Sample IPv6 IPs in data:`, ipv6Data.slice(0, 3).map(pv => pv.ip_address));
          }
          
        } catch (attributionError) {
          console.error('❌ Attribution data fetch error:', attributionError);
          allPageViews = [];
        }
      }
      
      // Fetch conversion data with deduplication
      let allConversions = [];
      if (conversionKeys.length > 0) {
        try {
          const conversionData = await redis(`mget/${conversionKeys.join('/')}`);
          const rawConversions = (conversionData.result || [])
            .filter(item => item)
            .map(item => {
              try {
                return JSON.parse(item);
              } catch (parseError) {
                return null;
              }
            })
            .filter(item => item)
            .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
          
          // Deduplicate by email
          const seenEmails = new Set();
          allConversions = rawConversions.filter(conversion => {
            if (!conversion.email) return true;
            if (seenEmails.has(conversion.email)) return false;
            seenEmails.add(conversion.email);
            return true;
          });
          
          console.log(`📊 Conversions: ${rawConversions.length} → ${allConversions.length} after deduplication`);
          
        } catch (conversionError) {
          console.error('❌ Conversion data fetch error:', conversionError);
          allConversions = [];
        }
      }
      
      console.log(`📊 Analytics query returned ${allPageViews.length} page views and ${allConversions.length} conversions`);
      
      // Apply filters
      let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
      let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
      
      console.log(`📊 After filtering: ${filteredPageViews.length} page views and ${filteredConversions.length} conversions`);
      
      // TASK 4.3: Include attribution stats if requested
      let attributionStatsData = null;
      if (include_attribution_stats === 'true') {
        try {
          console.log('📈 Including attribution stats in response...');
          attributionStatsData = await fetchAttributionStats(redis);
          console.log(`✅ Fetched ${attributionStatsData.length} attribution stat records`);
        } catch (statsError) {
          console.error('❌ Failed to fetch attribution stats:', statsError);
          attributionStatsData = [];
        }
      }
      
      // Calculate analytics
      const totalConversions = filteredConversions.length;
      const totalPageViews = filteredPageViews.length;
      
      const uniqueVisitorIPs = new Set();
      filteredPageViews.forEach(pv => {
        if (pv.ip_address && pv.ip_address !== 'unknown') {
          uniqueVisitorIPs.add(pv.ip_address);
        }
      });
      const uniqueVisitors = uniqueVisitorIPs.size;
      
      const paidConversions = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) > 0);
      const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
      const avgOrderValue = paidConversions.length > 0 ? totalRevenue / paidConversions.length : 0;
      const conversionRate = uniqueVisitors > 0 ? (totalConversions / uniqueVisitors * 100) : 0;
      
      // Build performance data
      const trafficSources = {};
      const campaignPerformance = {};
      const landingPageStats = {};
      
      // Process page views
      filteredPageViews.forEach(item => {
        const source = item.source || 'direct';
        const campaign = item.utm_campaign || item.campaign || 'none';
        const landingPage = item.landing_page || item.page_url || 'unknown';
        
        if (!trafficSources[source]) {
          trafficSources[source] = { 
            pageViews: 0, conversions: 0, revenue: 0, uniqueVisitors: new Set() 
          };
        }
        trafficSources[source].pageViews++;
        if (item.ip_address) {
          trafficSources[source].uniqueVisitors.add(item.ip_address);
        }
        
        if (!campaignPerformance[campaign]) {
          campaignPerformance[campaign] = { 
            pageViews: 0, conversions: 0, revenue: 0, uniqueVisitors: new Set()
          };
        }
        campaignPerformance[campaign].pageViews++;
        if (item.ip_address) {
          campaignPerformance[campaign].uniqueVisitors.add(item.ip_address);
        }
        
        if (!landingPageStats[landingPage]) {
          landingPageStats[landingPage] = { 
            pageViews: 0, conversions: 0, revenue: 0, uniqueVisitors: new Set() 
          };
        }
        landingPageStats[landingPage].pageViews++;
        if (item.ip_address) {
          landingPageStats[landingPage].uniqueVisitors.add(item.ip_address);
        }
      });
      
      // Process conversions
      filteredConversions.forEach(item => {
        const source = item.source || 'direct';
        const campaign = item.utm_campaign || item.campaign || 'none';
        const landingPage = item.landing_page || item.page_url || 'unknown';
        const revenue = parseFloat(item.order_total) || 0;
        
        if (trafficSources[source]) {
          trafficSources[source].conversions++;
          trafficSources[source].revenue += revenue;
        }
        
        if (campaignPerformance[campaign]) {
          campaignPerformance[campaign].conversions++;
          campaignPerformance[campaign].revenue += revenue;
        }
        
        if (landingPageStats[landingPage]) {
          landingPageStats[landingPage].conversions++;
          landingPageStats[landingPage].revenue += revenue;
        }
      });
      
      // Format response data
      const topSources = Object.entries(trafficSources)
        .map(([source, data]) => ({
          source,
          pageViews: data.pageViews,
          uniqueVisitors: data.uniqueVisitors.size,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.uniqueVisitors.size > 0 ? 
            (data.conversions / data.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews);
      
      const topCampaigns = Object.entries(campaignPerformance)
        .map(([campaign, data]) => ({
          campaign,
          pageViews: data.pageViews,
          uniqueVisitors: data.uniqueVisitors.size,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.uniqueVisitors.size > 0 ? 
            (data.conversions / data.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews);
      
      const topLandingPages = Object.entries(landingPageStats)
        .map(([page, data]) => ({
          landing_page: page,
          pageViews: data.pageViews,
          uniqueVisitors: data.uniqueVisitors.size,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.uniqueVisitors.size > 0 ? 
            (data.conversions / data.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews);
      
      // Daily trends
      const dailyStats = {};
      
      filteredPageViews.forEach(item => {
        const date = new Date(item.timestamp).toISOString().split('T')[0];
        if (!dailyStats[date]) {
          dailyStats[date] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        dailyStats[date].pageViews++;
        if (item.ip_address) {
          dailyStats[date].uniqueVisitors.add(item.ip_address);
        }
      });
      
      filteredConversions.forEach(item => {
        const date = new Date(item.timestamp).toISOString().split('T')[0];
        if (!dailyStats[date]) {
          dailyStats[date] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set() };
        }
        dailyStats[date].conversions++;
      });
      
      const dailyTrends = Object.entries(dailyStats)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, data]) => ({
          date,
          pageViews: data.pageViews,
          conversions: data.conversions,
          uniqueVisitors: data.uniqueVisitors.size,
          conversionRate: data.uniqueVisitors.size > 0 ? 
            (data.conversions / data.uniqueVisitors.size * 100).toFixed(1) : '0.0'
        }));
      
      // Include IPv6 stats in debug
      const ipv4KeysCount = attributionKeys.filter(key => {
        const parts = key.split('_');
        return parts.length === 6 && /^\d+$/.test(parts[1]) && /^\d+$/.test(parts[2]);
      }).length;
      const ipv6KeysCount = attributionKeys.filter(key => {
        const parts = key.split('_');
        return parts.length > 6;
      }).length;
      
      // Build response object
      const response = {
        summary: {
          total_page_views: totalPageViews,
          unique_visitors: uniqueVisitors,
          total_conversions: totalConversions,
          free_trials: filteredConversions.filter(c => (parseFloat(c.order_total) || 0) === 0).length,
          paid_conversions: paidConversions.length,
          total_revenue: totalRevenue,
          avg_order_value: avgOrderValue,
          conversion_rate: conversionRate.toFixed(1),
          date_range: { start: start_date, end: end_date }
        },
        traffic_sources: topSources,
        campaign_performance: topCampaigns,
        landing_page_performance: topLandingPages,
        daily_trends: dailyTrends,
        conversions: filteredConversions,
        page_views: filteredPageViews,
        
        debug: {
          attribution_keys_found: attributionKeys.length,
          ipv4_keys_found: ipv4KeysCount,
          ipv6_keys_found: ipv6KeysCount,
          conversion_keys_found: conversionKeys.length,
          sample_attribution_key: attributionKeys[0] || 'none',
          sample_ipv6_key: attributionKeys.find(k => k.split('_').length > 6) || 'none',
          deployment_timestamp: new Date().toISOString(),
          redis_method: 'hybrid_scan_keys_approach'
        }
      };
      
      // TASK 4.3: Add attribution stats to response if requested
      if (attributionStatsData !== null) {
        response.attribution_stats = attributionStatsData;
        response.attribution_summary = calculateAttributionSummary(attributionStatsData, filteredConversions);
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(response)
      };
      
    } catch (error) {
      console.error('❌ Analytics GET error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'POST') {
    try {
      const data = JSON.parse(event.body);
      
      if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
        const key = data.email ? 
          `conversions:${data.email.replace(/[^a-zA-Z0-9]/g, '_')}:${Date.now()}` :
          `conversions:${data.timestamp}:${Math.random()}`;
        
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
        console.log(`✅ Stored conversion: ${data.email || 'no email'}`);
      } else {
        const key = `pageviews:${data.timestamp}:${Math.random()}`;
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
        console.log(`✅ Stored page view: ${data.source} → ${data.landing_page}`);
      }
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ success: true })
      };
      
    } catch (error) {
      console.error('❌ Analytics POST error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

// TASK 4.3: Fetch attribution stats from Redis
async function fetchAttributionStats(redis) {
  try {
    const statsKeys = await redis('keys/attribution_stats_*');
    const attributionStats = [];
    
    if (statsKeys.result && statsKeys.result.length > 0) {
      // Sort keys by timestamp (newest first) and limit to last 200 records for performance
      const sortedKeys = statsKeys.result
        .sort((a, b) => {
          const timestampA = parseInt(a.split('_').pop()) || 0;
          const timestampB = parseInt(b.split('_').pop()) || 0;
          return timestampB - timestampA;
        })
        .slice(0, 200);
      
      console.log(`📈 Fetching ${sortedKeys.length} attribution stats records...`);
      
      for (const key of sortedKeys) {
        try {
          const statsData = await redis(`get/${key}`);
          if (statsData.result) {
            const parsedStats = JSON.parse(decodeURIComponent(statsData.result));
            attributionStats.push(parsedStats);
          }
        } catch (parseError) {
          console.log(`⚠️ Failed to parse attribution stats key: ${key}`);
          continue;
        }
      }
    }
    
    return attributionStats;
  } catch (error) {
    console.error('❌ Error fetching attribution stats:', error);
    return [];
  }
}

// TASK 4.3: Calculate attribution summary metrics
function calculateAttributionSummary(attributionStats, conversions) {
  const totalStats = attributionStats.length;
  const successfulAttributions = attributionStats.filter(stat => stat.success).length;
  const attributionRate = totalStats > 0 ? Math.round(successfulAttributions / totalStats * 100) : 0;
  
  // Method breakdown
  const methodBreakdown = {};
  attributionStats.forEach(stat => {
    const method = stat.method || 'none';
    methodBreakdown[method] = (methodBreakdown[method] || 0) + 1;
  });
  
  // Average score for successful attributions
  const successfulStats = attributionStats.filter(stat => stat.success && stat.score > 0);
  const avgScore = successfulStats.length > 0 
    ? Math.round(successfulStats.reduce((sum, stat) => sum + stat.score, 0) / successfulStats.length)
    : 0;
  
  // IPv6/IPv4 breakdown in conversions
  const ipv6Conversions = conversions.filter(c => c.ip_address && c.ip_address.includes(':')).length;
  const ipv4Conversions = conversions.filter(c => c.ip_address && !c.ip_address.includes(':')).length;
  
  // Geographic correlation success
  const geoStats = attributionStats.filter(stat => stat.method && stat.method.includes('geo'));
  const geoSuccessRate = geoStats.length > 0 
    ? Math.round(geoStats.filter(stat => stat.success).length / geoStats.length * 100)
    : 0;
  
  return {
    attribution_rate: attributionRate,
    total_attribution_attempts: totalStats,
    successful_attributions: successfulAttributions,
    method_breakdown: methodBreakdown,
    average_attribution_score: avgScore,
    ipv6_conversions: ipv6Conversions,
    ipv4_conversions: ipv4Conversions,
    geographic_correlation_success_rate: geoSuccessRate,
    last_updated: new Date().toISOString()
  };
}

// TASK 5.2: Calculate attribution health metrics
async function calculateAttributionHealth(redis) {
  const last24Hours = Date.now() - (24 * 60 * 60 * 1000);
  
  try {
    // Get recent conversions
    const conversionKeys = await redis('keys/conversions:*');
    let recentConversions = [];
    
    if (conversionKeys.result && conversionKeys.result.length > 0) {
      // Get last 100 conversions for performance
      const recentKeys = conversionKeys.result
        .sort((a, b) => {
          const timestampA = extractTimestampFromKey(a);
          const timestampB = extractTimestampFromKey(b);
          return timestampB - timestampA;
        })
        .slice(0, 100);
      
      for (const key of recentKeys) {
        try {
          const conv = await redis(`get/${key}`);
          if (conv.result) {
            const convData = JSON.parse(decodeURIComponent(conv.result));
            const convTimestamp = new Date(convData.timestamp).getTime();
            
            if (convTimestamp > last24Hours) {
              recentConversions.push(convData);
            }
          }
        } catch (parseError) {
          continue;
        }
      }
    }
    
    // Get recent attribution stats
    const statsKeys = await redis('keys/attribution_stats_*');
    let recentStats = [];
    
    if (statsKeys.result && statsKeys.result.length > 0) {
      const recentStatsKeys = statsKeys.result
        .filter(key => {
          const timestamp = parseInt(key.split('_').pop()) || 0;
          return timestamp > last24Hours;
        })
        .sort((a, b) => {
          const timestampA = parseInt(a.split('_').pop()) || 0;
          const timestampB = parseInt(b.split('_').pop()) || 0;
          return timestampB - timestampA;
        });
      
      for (const key of recentStatsKeys) {
        try {
          const stat = await redis(`get/${key}`);
          if (stat.result) {
            const statData = JSON.parse(decodeURIComponent(stat.result));
            recentStats.push(statData);
          }
        } catch (parseError) {
          continue;
        }
      }
    }
    
    const totalConversions = recentConversions.length;
    const successfulAttributions = recentConversions.filter(c => c.attribution_found).length;
    const successRate = totalConversions > 0 ? Math.round(successfulAttributions / totalConversions * 100) : 0;
    
    // Method performance
    const methodStats = {};
    recentStats.forEach(stat => {
      const method = stat.method || 'none';
      if (!methodStats[method]) {
        methodStats[method] = { total: 0, successful: 0 };
      }
      methodStats[method].total++;
      if (stat.success) {
        methodStats[method].successful++;
      }
    });
    
    // IPv6/IPv4 dual-stack correlation performance
    const ipv6Pageviews = recentStats.filter(stat => 
      stat.customer_ip && stat.customer_ip.includes(':')
    ).length;
    
    const geoCorrelationAttempts = recentStats.filter(stat => 
      stat.method && stat.method.includes('geo')
    ).length;
    
    const geoCorrelationSuccesses = recentStats.filter(stat => 
      stat.method && stat.method.includes('geo') && stat.success
    ).length;
    
    const geoSuccessRate = geoCorrelationAttempts > 0 
      ? Math.round(geoCorrelationSuccesses / geoCorrelationAttempts * 100)
      : 0;
    
    // Health status determination
    let status = 'healthy';
    let alerts = [];
    
    if (successRate < 70) {
      status = 'critical';
      alerts.push(`Attribution success rate at ${successRate}% (target: 80%+)`);
    } else if (successRate < 80) {
      status = 'warning';
      alerts.push(`Attribution success rate at ${successRate}% (target: 80%+)`);
    }
    
    if (geoCorrelationAttempts > 0 && geoSuccessRate < 60) {
      alerts.push(`Geographic correlation success rate at ${geoSuccessRate}% (target: 80%+)`);
      if (status === 'healthy') status = 'warning';
    }
    
    if (ipv6Pageviews > 0 && geoCorrelationAttempts === 0) {
      alerts.push('IPv6 traffic detected but no geographic correlation attempts');
      if (status === 'healthy') status = 'warning';
    }
    
    return {
      status,
      success_rate: successRate,
      total_conversions: totalConversions,
      successful_attributions: successfulAttributions,
      method_performance: methodStats,
      geographic_correlation: {
        attempts: geoCorrelationAttempts,
        successes: geoCorrelationSuccesses,
        success_rate: geoSuccessRate
      },
      ipv6_metrics: {
        pageviews: ipv6Pageviews,
        dual_stack_ready: geoCorrelationAttempts > 0
      },
      alerts,
      timestamp: new Date().toISOString(),
      time_window: '24 hours'
    };
    
  } catch (error) {
    console.error('❌ Attribution health calculation error:', error);
    return {
      status: 'error',
      success_rate: 0,
      total_conversions: 0,
      successful_attributions: 0,
      error: error.message,
      timestamp: new Date().toISOString()
    };
  }
}

// Helper function to extract timestamp from Redis key
function extractTimestampFromKey(key) {
  const parts = key.split(':');
  if (parts.length >= 2) {
    const timestamp = new Date(parts[1]).getTime();
    return isNaN(timestamp) ? 0 : timestamp;
  }
  return 0;
}

function applyFilters(data, filters) {
  let filtered = data;
  
  if (filters.start_date) {
    const startDate = new Date(filters.start_date);
    filtered = filtered.filter(item => new Date(item.timestamp) >= startDate);
  }
  
  if (filters.end_date) {
    const endDate = new Date(filters.end_date);
    endDate.setHours(23, 59, 59, 999);
    filtered = filtered.filter(item => new Date(item.timestamp) <= endDate);
  }
  
  if (filters.source) {
    filtered = filtered.filter(item => item.source === filters.source);
  }
  
  if (filters.campaign) {
    filtered = filtered.filter(item => 
      (item.utm_campaign || item.campaign) === filters.campaign
    );
  }
  
  return filtered;
}

module.exports = { handler };
