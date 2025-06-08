// File: netlify/functions/analytics.js
// Enhanced Analytics API endpoint for free trial tracking

// In-memory storage for demo (use database in production)
let conversionStore = new Map();
let pageViewStore = new Map();

const handler = async (event, context) => {
  // Handle CORS preflight
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

  if (event.httpMethod === 'GET') {
    // Return analytics data including free trial conversions
    try {
      const { start_date, end_date, source, campaign, type } = event.queryStringParameters || {};
      
      console.log(`📊 Analytics GET request - Filters: start=${start_date}, end=${end_date}, source=${source}, campaign=${campaign}`);
      
      // Get all stored conversions (including $0 free trials)
      const allConversions = Array.from(conversionStore.values())
        .filter(item => item.event_type === 'purchase' || item.event_type === 'conversion')
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      console.log(`📊 Found ${allConversions.length} total conversions in store`);
      
      // Get all stored page views
      const allPageViews = Array.from(pageViewStore.values())
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      console.log(`📊 Found ${allPageViews.length} total page views in store`);
      
      // Apply filters to both datasets
      let filteredConversions = applyFilters(allConversions, { start_date, end_date, source, campaign });
      let filteredPageViews = applyFilters(allPageViews, { start_date, end_date, source, campaign });
      
      console.log(`📊 After filtering: ${filteredConversions.length} conversions, ${filteredPageViews.length} page views`);
      
      // Calculate comprehensive analytics
      const totalConversions = filteredConversions.length;
      const totalPageViews = filteredPageViews.length;
      
      // For free trial business: all conversions are trials, revenue tracking is secondary
      const freeTrials = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) === 0);
      const paidConversions = filteredConversions.filter(item => (parseFloat(item.order_total) || 0) > 0);
      
      const totalRevenue = filteredConversions.reduce((sum, item) => 
        sum + (parseFloat(item.order_total) || 0), 0
      );
      
      const avgOrderValue = paidConversions.length > 0 ? totalRevenue / paidConversions.length : 0;
      const conversionRate = totalPageViews > 0 ? (totalConversions / totalPageViews * 100) : 0;
      
      // Unique visitors (deduplicated by IP address)
      const uniqueVisitors = new Set(filteredPageViews.map(item => item.ip_address)).size;
      
      // Traffic source analysis (page views)
      const trafficSources = {};
      const campaignPerformance = {};
      const landingPageStats = {};
      
      filteredPageViews.forEach(item => {
        const source = item.source || 'direct';
        const campaign = item.campaign || 'none';
        const landingPage = item.landing_page || item.page_url || 'unknown';
        
        // Traffic sources
        if (!trafficSources[source]) {
          trafficSources[source] = { pageViews: 0, conversions: 0, revenue: 0 };
        }
        trafficSources[source].pageViews++;
        
        // Campaigns
        if (!campaignPerformance[campaign]) {
          campaignPerformance[campaign] = { pageViews: 0, conversions: 0, revenue: 0 };
        }
        campaignPerformance[campaign].pageViews++;
        
        // Landing pages
        if (!landingPageStats[landingPage]) {
          landingPageStats[landingPage] = { pageViews: 0, conversions: 0, revenue: 0, uniqueVisitors: new Set() };
        }
        landingPageStats[landingPage].pageViews++;
        landingPageStats[landingPage].uniqueVisitors.add(item.ip_address);
      });
      
      // Add conversion data to sources/campaigns/pages (including $0 conversions)
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
      
      // Calculate conversion rates and format data
      const topSources = Object.entries(trafficSources)
        .map(([source, data]) => ({
          source,
          pageViews: data.pageViews,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews)
        .slice(0, 10);
      
      const topCampaigns = Object.entries(campaignPerformance)
        .map(([campaign, data]) => ({
          campaign,
          pageViews: data.pageViews,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews)
        .slice(0, 10);
      
      const topLandingPages = Object.entries(landingPageStats)
        .map(([page, data]) => ({
          landing_page: page,
          pageViews: data.pageViews,
          uniqueVisitors: data.uniqueVisitors.size,
          conversions: data.conversions,
          revenue: data.revenue,
          conversionRate: data.pageViews > 0 ? (data.conversions / data.pageViews * 100).toFixed(1) : '0.0'
        }))
        .sort((a, b) => b.pageViews - a.pageViews)
        .slice(0, 10);
      
      // Daily trends for both page views and conversions
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
      
      const analytics = {
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
        conversions: filteredConversions.map(item => ({
          timestamp: item.timestamp,
          source: item.source,
          campaign: item.campaign,
          content: item.content,
          medium: item.medium,
          landing_page: item.landing_page,
          email: item.email,
          name: item.name,
          order_total: item.order_total || 0,
          offer_name: item.offer_name,
          order_id: item.order_id,
          subscription_id: item.subscription_id,
          attribution_found: item.attribution_found,
          attribution_source: item.attribution_source,
          is_free_trial: (parseFloat(item.order_total) || 0) === 0
        })),
        page_views: filteredPageViews.map(item => ({
          timestamp: item.timestamp,
          source: item.source,
          campaign: item.campaign,
          landing_page: item.landing_page || item.page_url,
          ip_address: item.ip_address,
          user_agent: item.user_agent,
          referrer_url: item.referrer_url
        }))
      };
      
      console.log(`📊 Analytics response: ${totalPageViews} page views, ${totalConversions} conversions (${freeTrials.length} free trials + ${paidConversions.length} paid)`);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(analytics)
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
    // Store both conversion and page view data
    try {
      const data = JSON.parse(event.body);
      
      console.log(`📥 Analytics POST received - Event type: ${data.event_type}`);
      console.log(`📥 Data: ${JSON.stringify(data, null, 2)}`);
      
      if (data.event_type === 'purchase' || data.event_type === 'conversion' || data.order_total !== undefined) {
        // Store conversion data (including $0 free trials)
        const key = `conversion:${data.timestamp}:${Math.random()}`;
        conversionStore.set(key, data);
        console.log(`📊 Stored conversion: ${data.email} - Order total: $${data.order_total || 0} - Free trial: ${(parseFloat(data.order_total) || 0) === 0}`);
      } else {
        // Store page view data
        const key = `pageview:${data.timestamp}:${Math.random()}`;
        pageViewStore.set(key, {
          ...data,
          event_type: 'page_view'
        });
        console.log(`📊 Stored page view: ${data.source} → ${data.landing_page}`);
      }
      
      // Memory management - keep more conversions since they're valuable
      if (conversionStore.size > 2000) {
        const entries = Array.from(conversionStore.entries()).slice(-1500);
        conversionStore.clear();
        entries.forEach(([key, value]) => conversionStore.set(key, value));
        console.log('🧹 Cleaned up conversion store');
      }
      
      if (pageViewStore.size > 3000) {
        const entries = Array.from(pageViewStore.entries()).slice(-2000);
        pageViewStore.clear();
        entries.forEach(([key, value]) => pageViewStore.set(key, value));
        console.log('🧹 Cleaned up page view store');
      }
      
      console.log(`📊 Current store sizes: ${conversionStore.size} conversions, ${pageViewStore.size} page views`);
      
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

// Helper function to apply filters
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
