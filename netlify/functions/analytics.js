// File: netlify/functions/analytics.js
// Fixed Redis-powered analytics API - Now reads attribution data correctly

const handler = async (event, context) => {
  // Handle CORS preflight
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

  // 🔒 Security Check - Verify API Key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!validApiKey) {
    console.error('❌ No API key configured in environment');
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Server configuration error' })
    };
  }

  if (!apiKey || apiKey !== validApiKey) {
    console.log('🚫 Unauthorized access attempt');
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  console.log('✅ API key validated');

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!redisUrl || !redisToken) {
    console.error('❌ Missing Redis credentials');
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Redis not configured' })
    };
  }

  // Redis helper function
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
      
      // 🔧 FIXED: Get attribution data and conversions from the correct Redis keys
      const [attributionResult, conversionsResult] = await Promise.all([
        redis('keys/attribution:*'),  // ✅ Read attribution data
        redis('keys/conversions:*')   // Keep conversions as is
      ]);
      
      const attributionKeys = attributionResult.result || [];
      const conversionKeys = conversionsResult.result || [];
      
      console.log(`📊 Found ${attributionKeys.length} attribution keys and ${conversionKeys.length} conversion keys`);
      
      // Fetch all attribution data (page views)
      let allPageViews = [];
      if (attributionKeys.length > 0) {
        const attributionData = await redis(`mget/${attributionKeys.join('/')}`);
        allPageViews = (attributionData.result || [])
          .filter(item => item)
          .map(item => JSON.parse(item))
          .map(item => ({
            ...item,
            event_type: 'page_view'  // Ensure it's marked as page view
          }))
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      }
      
      // Fetch all conversion data
      let allConversions = [];
      if (conversionKeys.length > 0) {
        const conversionData = await redis(`mget/${conversionKeys.join('/')}`);
        allConversions = (conversionData.result || [])
          .filter(item => item)
          .map(item => JSON.parse(item))
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      }
      
      console.log(`📊 Analytics query returned ${allPageViews.length} page views and ${allConversions.length} conversions`);
      
      // Apply filters
      let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
      let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
      
      console.log(`📊 After filtering: ${filteredPageViews.length} page views and ${filteredConversions.length} conversions`);
      
      // Calculate analytics (same logic as before)
      const totalConversions = filteredConversions.length;
      const totalPageViews = filteredPageViews.length;
      const freeTrials = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) === 0);
      const paidConversions = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) > 0);
      const totalRevenue = filteredConversions.reduce((sum, item) => sum + (parseFloat(item.order_total) || 0), 0);
      const avgOrderValue = paidConversions.length > 0 ? totalRevenue / paidConversions.length : 0;
      const uniqueVisitors = new Set(filteredPageViews.map(item => item.ip_address)).size;
      const conversionRate = uniqueVisitors > 0 ? (totalConversions / uniqueVisitors * 100) : 0;
      
      // Build traffic sources, campaigns, landing pages (same logic)
      const trafficSources = {};
      const campaignPerformance = {};
      const landingPageStats = {};
      
      filteredPageViews.forEach(item => {
        const source = item.source || 'direct';
        const campaign = item.utm_campaign || item.campaign || 'none';  // ✅ Use utm_campaign if available
        const landingPage = item.landing_page || item.page_url || 'unknown';
        
        if (!trafficSources[source]) {
          trafficSources[source] = { pageViews: 0, conversions: 0, revenue: 0 };
        }
        trafficSources[source].pageViews++;
        
        if (!campaignPerformance[campaign]) {
          campaignPerformance[campaign] = { pageViews: 0, conversions: 0, revenue: 0 };
        }
        campaignPerformance[campaign].pageViews++;
        
        if (!landingPageStats[landingPage]) {
          landingPageStats[landingPage] = { pageViews: 0, conversions: 0, revenue: 0, uniqueVisitors: new Set() };
        }
        landingPageStats[landingPage].pageViews++;
        landingPageStats[landingPage].uniqueVisitors.add(item.ip_address);
      });
      
      filteredConversions.forEach(item => {
        const source = item.source || 'direct';
        const campaign = item.utm_campaign || item.campaign || 'none';  // ✅ Use utm_campaign if available
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
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews);
      
      const topCampaigns = Object.entries(campaignPerformance)
        .map(([campaign, data]) => ({
          campaign,
          pageViews: data.pageViews,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews);
      
      const topLandingPages = Object.entries(landingPageStats)
        .map(([page, data]) => ({
          landing_page: page,
          pageViews: data.pageViews,
          uniqueVisitors: data.uniqueVisitors.size,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
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
        dailyStats[date].uniqueVisitors.add(item.ip_address);
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
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }));
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({
          summary: {
            total_page_views: totalPageViews,
            unique_visitors: uniqueVisitors,
            total_conversions: totalConversions,
            free_trials: freeTrials.length,
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
          page_views: filteredPageViews
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
  
  if (event.httpMethod === 'POST') {
    try {
      const data = JSON.parse(event.body);
      
      console.log(`📥 Storing ${data.event_type}: ${data.email || 'no email'}`);
      
      if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
        // Store conversion
        const key = `conversions:${data.timestamp}:${Math.random()}`;
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(data))}`);
        console.log(`✅ Stored conversion: ${data.email}`);
      } else {
        // Store page view - Keep this for future data, but analytics now reads from attribution keys
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
    filtered = filtered.filter(item => (item.utm_campaign || item.campaign) === filters.campaign);
  }
  
  return filtered;
}

module.exports = { handler };
