// File: netlify/functions/analytics.js  
// FIXED: IPv6 support and proper Redis key reading

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

  // Security check
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

  if (event.httpMethod === 'GET') {
    try {
      const { start_date, end_date, source, campaign } = event.queryStringParameters || {};
      
      console.log(`📊 Analytics query: start=${start_date}, end=${end_date}, source=${source}, campaign=${campaign}`);
      
      // FIXED: Better Redis key fetching with IPv6 support
      let attributionKeys = [];
      let conversionKeys = [];
      
      try {
        // Get attribution keys - using IPv6-safe pattern
        const attributionResult = await redis('keys/attribution_*');
        attributionKeys = attributionResult.result || [];
        console.log(`🔍 Raw attribution keys found: ${attributionKeys.length}`);
        
        // Debug: Log first few keys to see the pattern
        if (attributionKeys.length > 0) {
          console.log(`📋 Sample attribution keys:`, attributionKeys.slice(0, 3));
        }
        
        // Get conversion keys
        const conversionsResult = await redis('keys/conversions:*');
        conversionKeys = conversionsResult.result || [];
        console.log(`🔍 Raw conversion keys found: ${conversionKeys.length}`);
        
      } catch (redisError) {
        console.error('❌ Redis key fetch error:', redisError);
        // Continue with empty arrays rather than failing completely
        attributionKeys = [];
        conversionKeys = [];
      }
      
      console.log(`📊 Found ${attributionKeys.length} attribution keys and ${conversionKeys.length} conversion keys`);
      
      // Fetch attribution data (page views) with error handling
      let allPageViews = [];
      if (attributionKeys.length > 0) {
        try {
          // Batch fetch attribution data - handle IPv6 keys carefully
          const chunkSize = 50; // Process in smaller chunks to avoid issues
          
          for (let i = 0; i < attributionKeys.length; i += chunkSize) {
            const chunk = attributionKeys.slice(i, i + chunkSize);
            console.log(`📦 Processing attribution chunk ${Math.floor(i/chunkSize) + 1}/${Math.ceil(attributionKeys.length/chunkSize)}`);
            
            try {
              const chunkResult = await redis(`mget/${chunk.join('/')}`);
              const chunkData = (chunkResult.result || [])
                .filter(item => item) // Remove null/undefined items
                .map(item => {
                  try {
                    return JSON.parse(item);
                  } catch (parseError) {
                    console.log('⚠️ Failed to parse attribution item:', parseError);
                    return null;
                  }
                })
                .filter(item => item) // Remove failed parses
                .map(item => ({ ...item, event_type: 'page_view' }));
              
              allPageViews = allPageViews.concat(chunkData);
              
            } catch (chunkError) {
              console.log(`⚠️ Failed to fetch attribution chunk:`, chunkError);
              // Continue with other chunks
            }
          }
          
          console.log(`📊 Successfully parsed ${allPageViews.length} page views`);
          
        } catch (attributionError) {
          console.error('❌ Attribution data fetch error:', attributionError);
          allPageViews = [];
        }
      }
      
      // Fetch conversion data with PROPER DEDUPLICATION
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
                console.log('⚠️ Failed to parse conversion item:', parseError);
                return null;
              }
            })
            .filter(item => item)
            .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
          
          // CRITICAL FIX: Deduplicate conversions by email
          const seenEmails = new Set();
          allConversions = rawConversions.filter(conversion => {
            if (!conversion.email) return true; // Keep conversions without email
            
            if (seenEmails.has(conversion.email)) {
              console.log(`🚫 Removing duplicate conversion for: ${conversion.email}`);
              return false;
            }
            
            seenEmails.add(conversion.email);
            return true;
          });
          
          console.log(`📊 Conversions: ${rawConversions.length} → ${allConversions.length} after deduplication`);
          
        } catch (conversionError) {
          console.error('❌ Conversion data fetch error:', conversionError);
          allConversions = [];
        }
      }
      
      // Sort page views by timestamp
      allPageViews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      console.log(`📊 Analytics query returned ${allPageViews.length} page views and ${allConversions.length} conversions`);
      
      // Apply filters
      let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
      let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
      
      console.log(`📊 After filtering: ${filteredPageViews.length} page views and ${filteredConversions.length} conversions`);
      
      // Calculate analytics
      const totalConversions = filteredConversions.length;
      const totalPageViews = filteredPageViews.length;
      
      // Deduplicate page views by IP for unique visitors count
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
        
        // Traffic sources
        if (!trafficSources[source]) {
          trafficSources[source] = { 
            pageViews: 0, 
            conversions: 0, 
            revenue: 0, 
            uniqueVisitors: new Set() 
          };
        }
        trafficSources[source].pageViews++;
        if (item.ip_address) {
          trafficSources[source].uniqueVisitors.add(item.ip_address);
        }
        
        // Campaign performance
        if (!campaignPerformance[campaign]) {
          campaignPerformance[campaign] = { 
            pageViews: 0, 
            conversions: 0, 
            revenue: 0,
            uniqueVisitors: new Set()
          };
        }
        campaignPerformance[campaign].pageViews++;
        if (item.ip_address) {
          campaignPerformance[campaign].uniqueVisitors.add(item.ip_address);
        }
        
        // Landing pages
        if (!landingPageStats[landingPage]) {
          landingPageStats[landingPage] = { 
            pageViews: 0, 
            conversions: 0, 
            revenue: 0, 
            uniqueVisitors: new Set() 
          };
        }
        landingPageStats[landingPage].pageViews++;
        if (item.ip_address) {
          landingPageStats[landingPage].uniqueVisitors.add(item.ip_address);
        }
      });
      
      // Process conversions (deduplicated)
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
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({
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
          conversions: filteredConversions, // Already deduplicated
          page_views: filteredPageViews,
          
          // Debug info
          debug: {
            attribution_keys_found: attributionKeys.length,
            conversion_keys_found: conversionKeys.length,
            sample_attribution_key: attributionKeys[0] || 'none'
          }
        })
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
  
  // Handle POST requests (for storing data)
  if (event.httpMethod === 'POST') {
    try {
      const data = JSON.parse(event.body);
      
      if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
        // Use email-based key for conversions to prevent duplicates
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

// Apply date and criteria filters
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
