// File: netlify/functions/analytics.js
// Redis-powered analytics API

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
      }
    };
  }

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
      
      // Get all conversions and page views from Redis
      const [conversionsResult, pageViewsResult] = await Promise.all([
        redis('keys/conversions:*'),
        redis('keys/pageviews:*')
      ]);
      
      const conversionKeys = conversionsResult.result || [];
      const pageViewKeys = pageViewsResult.result || [];
      
      // Fetch all conversion data
      let allConversions = [];
      if (conversionKeys.length > 0) {
        const conversionData = await redis(`mget/${conversionKeys.join('/')}`);
        allConversions = (conversionData.result || [])
          .filter(item => item)
          .map(item => JSON.parse(item))
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      }
      
      // Fetch all page view data
      let allPageViews = [];
      if (pageViewKeys.length > 0) {
        const pageViewData = await redis(`mget/${pageViewKeys.join('/')}`);
        allPageViews = (pageViewData.result || [])
          .filter(item => item)
          .map(item => JSON.parse(item))
          .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      }
      
      console.log(`📊 Analytics query returned ${allConversions.length} conversions`);
      
      // Apply filters
      let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
      let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
      
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
        const campaign = item.campaign || 'none';
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
        const campaign = item.campaign || 'none';
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
        // Store page view
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
    filtered = filtered.filter(item => item.campaign === filters.campaign);
  }
  
  return filtered;
}

module.exports = { handler };
