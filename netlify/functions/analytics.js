// File: netlify/functions/analytics.js
// ENHANCED: Attribution Stats & Health Monitoring with Timestamp Validation

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
      console.error('❌ Health check error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'GET') {
    try {
      // Parse query parameters
      const { start_date, end_date, source, campaign, include_attribution_stats } = event.queryStringParameters || {};
      
      console.log(`📅 Analytics request: ${start_date || 'no start'} to ${end_date || 'no end'}`);
      console.log(`🎯 Filters: source=${source || 'all'}, campaign=${campaign || 'all'}`);
      
      // Get all keys from Redis
      let attributionKeys = [];
      let conversionKeys = [];
      
      try {
        // Use scan for better performance with large datasets
        console.log('🔍 Getting Redis keys...');
        
        try {
          // Try IPv6-optimized approach first
          const ipv6Prefixes = ['2600', '2601', '2603', '2604', '2605', '2606', '2607', '2800', '2001', '2002'];
          let totalAttributionKeys = [];
          
          for (const prefix of ipv6Prefixes) {
            try {
              const result = await redis(`keys/attribution_${prefix}*`);
              if (result.result && result.result.length > 0) {
                totalAttributionKeys = totalAttributionKeys.concat(result.result);
                console.log(`✅ Found ${result.result.length} IPv6 keys with prefix ${prefix}`);
              }
            } catch (prefixError) {
              // Continue to next prefix
            }
          }
          
          // Also get IPv4 keys
          try {
            const ipv4Result = await redis('keys/attribution_*');
            if (ipv4Result.result) {
              const ipv4Keys = ipv4Result.result.filter(key => {
                const parts = key.split('_');
                return parts.length === 4 && /^\d+$/.test(parts[1]) && /^\d+$/.test(parts[2]);
              });
              totalAttributionKeys = totalAttributionKeys.concat(ipv4Keys);
              console.log(`✅ Found ${ipv4Keys.length} IPv4 keys`);
            }
          } catch (ipv4Error) {
            console.log('⚠️ IPv4 key fetch failed:', ipv4Error);
          }
          
          // Remove duplicates
          attributionKeys = [...new Set(totalAttributionKeys)];
          console.log(`📊 Total unique attribution keys: ${attributionKeys.length}`);
          
        } catch (optimizedError) {
          console.log('⚠️ Optimized approach failed, trying emergency fallback...');
          
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
                      const parsed = JSON.parse(item);
                      // VALIDATE TIMESTAMP HERE
                      if (!isValidTimestamp(parsed.timestamp)) {
                        console.warn('⚠️ Invalid timestamp found in batch data:', parsed.timestamp);
                        parsed.timestamp = new Date().toISOString(); // Use current time as fallback
                      }
                      return parsed;
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
              .sort((a, b) => {
                const dateA = new Date(a.timestamp);
                const dateB = new Date(b.timestamp);
                return dateB - dateA;
              });
            
          } else {
            // Original code for smaller datasets
            const attributionData = await redis(`mget/${attributionKeys.join('/')}`);
            allPageViews = (attributionData.result || [])
              .filter(item => item)
              .map(item => {
                try {
                  const parsed = JSON.parse(item);
                  // VALIDATE TIMESTAMP HERE
                  if (!isValidTimestamp(parsed.timestamp)) {
                    console.warn('⚠️ Invalid timestamp found:', parsed.timestamp);
                    parsed.timestamp = new Date().toISOString(); // Use current time as fallback
                  }
                  return parsed;
                } catch (parseError) {
                  console.log('⚠️ Failed to parse attribution item');
                  return null;
                }
              })
              .filter(item => item)
              .map(item => ({ ...item, event_type: 'page_view' }))
              .sort((a, b) => {
                const dateA = new Date(a.timestamp);
                const dateB = new Date(b.timestamp);
                return dateB - dateA;
              });
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
                const parsed = JSON.parse(item);
                // VALIDATE TIMESTAMP HERE
                if (!isValidTimestamp(parsed.timestamp)) {
                  console.warn('⚠️ Invalid timestamp found in conversion:', parsed.timestamp);
                  parsed.timestamp = new Date().toISOString(); // Use current time as fallback
                }
                return parsed;
              } catch (parseError) {
                return null;
              }
            })
            .filter(item => item)
            .sort((a, b) => {
              const dateA = new Date(a.timestamp);
              const dateB = new Date(b.timestamp);
              return dateB - dateA;
            });
          
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
      
      // Performance tracking
      const trafficSourceStats = {};
      const campaignPerformance = {};
      const landingPageStats = {};
      
      filteredPageViews.forEach(item => {
        const source = item.source || 'unknown';
        const campaign = item.utm_campaign || item.campaign || 'unknown';
        const landingPage = item.landing_page || 'unknown';
        
        // Traffic source stats
        if (!trafficSourceStats[source]) {
          trafficSourceStats[source] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set(), revenue: 0 };
        }
        trafficSourceStats[source].pageViews++;
        if (item.ip_address) {
          trafficSourceStats[source].uniqueVisitors.add(item.ip_address);
        }
        
        // Campaign performance
        if (!campaignPerformance[campaign]) {
          campaignPerformance[campaign] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set(), revenue: 0 };
        }
        campaignPerformance[campaign].pageViews++;
        if (item.ip_address) {
          campaignPerformance[campaign].uniqueVisitors.add(item.ip_address);
        }
        
        // Landing page stats
        if (!landingPageStats[landingPage]) {
          landingPageStats[landingPage] = { pageViews: 0, conversions: 0, uniqueVisitors: new Set(), revenue: 0 };
        }
        landingPageStats[landingPage].pageViews++;
        if (item.ip_address) {
          landingPageStats[landingPage].uniqueVisitors.add(item.ip_address);
        }
      });
      
      filteredConversions.forEach(item => {
        const source = item.source || 'unknown';
        const campaign = item.utm_campaign || item.campaign || 'unknown';
        const landingPage = item.landing_page || 'unknown';
        const revenue = parseFloat(item.order_total) || 0;
        
        if (trafficSourceStats[source]) {
          trafficSourceStats[source].conversions++;
          trafficSourceStats[source].revenue += revenue;
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
      
      const topSources = Object.entries(trafficSourceStats)
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
      
      // Daily trends with timestamp validation
      const dailyStats = {};
      
      filteredPageViews.forEach(item => {
        if (!isValidTimestamp(item.timestamp)) {
          console.warn('⚠️ Skipping page view with invalid timestamp:', item.timestamp);
          return;
        }
        
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
        if (!isValidTimestamp(item.timestamp)) {
          console.warn('⚠️ Skipping conversion with invalid timestamp:', item.timestamp);
          return;
        }
        
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
        return parts.length === 4 && /^\d+$/.test(parts[1]) && /^\d+$/.test(parts[2]);
      }).length;
      const ipv6KeysCount = attributionKeys.filter(key => {
        const parts = key.split('_');
        return parts.length > 4;
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
          sample_ipv6_key: attributionKeys.find(k => k.split('_').length > 4) || 'none',
          deployment_timestamp: new Date().toISOString(),
          redis_method: 'timestamp_validated_approach'
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
      
      // Validate timestamp before storing
      if (!isValidTimestamp(data.timestamp)) {
        console.warn('⚠️ POST data has invalid timestamp, using current time:', data.timestamp);
        data.timestamp = new Date().toISOString();
      }
      
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

// =====================================================================
// HELPER FUNCTIONS
// =====================================================================

// NEW: Timestamp validation function
function isValidTimestamp(timestamp) {
  if (!timestamp) return false;
  
  // Try to parse the timestamp
  const date = new Date(timestamp);
  
  // Check if the date is valid and not NaN
  return date instanceof Date && !isNaN(date.getTime());
}

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
            
            // Validate timestamp in stats data
            if (!isValidTimestamp(parsedStats.timestamp)) {
              console.warn('⚠️ Invalid timestamp in attribution stats:', parsedStats.timestamp);
              parsedStats.timestamp = new Date().toISOString();
            }
            
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
  const attributionRate = totalStats > 0 ?
    Math.round(successfulAttributions / totalStats * 100) : 0;
  
  // Calculate the overall attribution rate for all conversions
  const totalConversions = conversions.length;
  const attributedConversions = conversions.filter(conv => 
    conv.attribution_found || conv.landing_page
  ).length;
  const overallAttributionRate = totalConversions > 0 ?
    Math.round(attributedConversions / totalConversions * 100) : 0;
  
  return {
    attribution_rate: attributionRate,
    total_attribution_attempts: totalStats,
    successful_attributions: successfulAttributions,
    overall_attribution_rate: overallAttributionRate,
    total_conversions: totalConversions,
    attributed_conversions: attributedConversions,
    last_updated: new Date().toISOString()
  };
}

// TASK 5.2: Calculate attribution system health
async function calculateAttributionHealth(redis) {
  try {
    console.log('🔬 Calculating attribution system health...');
    
    // Fetch recent attribution stats (last 50 records for performance)
    const recentStats = await fetchAttributionStats(redis);
    const last24Hours = recentStats.filter(stat => {
      if (!isValidTimestamp(stat.timestamp)) return false;
      const statTime = new Date(stat.timestamp).getTime();
      const dayAgo = Date.now() - (24 * 60 * 60 * 1000);
      return statTime > dayAgo;
    }).slice(0, 50);
    
    const totalConversions = last24Hours.length;
    const successfulAttributions = last24Hours.filter(stat => stat.success).length;
    const successRate = totalConversions > 0 ? 
      Math.round(successfulAttributions / totalConversions * 100) : 0;
    
    // Method performance
    const methodStats = {};
    last24Hours.forEach(stat => {
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
    const ipv6Pageviews = last24Hours.filter(stat => 
      stat.customer_ip && stat.customer_ip.includes(':')
    ).length;
    
    const geoCorrelationAttempts = last24Hours.filter(stat => 
      stat.method && stat.method.includes('geo')
    ).length;
    
    const geoCorrelationSuccesses = last24Hours.filter(stat => 
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
    filtered = filtered.filter(item => {
      if (!isValidTimestamp(item.timestamp)) return false;
      return new Date(item.timestamp) >= startDate;
    });
  }
  
  if (filters.end_date) {
    const endDate = new Date(filters.end_date);
    endDate.setHours(23, 59, 59, 999);
    filtered = filtered.filter(item => {
      if (!isValidTimestamp(item.timestamp)) return false;
      return new Date(item.timestamp) <= endDate;
    });
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
