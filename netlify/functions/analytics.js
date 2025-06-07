// File: netlify/functions/analytics.js
// Analytics API endpoint for dashboard

// This would normally connect to a database
// For now, we'll use the same in-memory store as attribution
let conversionStore = new Map();

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
    // Return analytics data
    try {
      const { start_date, end_date, source, campaign } = event.queryStringParameters || {};
      
      // Get all stored conversions (in production, this would be from a database)
      const allConversions = Array.from(conversionStore.values())
        .filter(item => item.event_type === 'purchase')
        .sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      
      // Apply filters
      let filteredConversions = allConversions;
      
      if (start_date) {
        const startDate = new Date(start_date);
        filteredConversions = filteredConversions.filter(item => 
          new Date(item.timestamp) >= startDate
        );
      }
      
      if (end_date) {
        const endDate = new Date(end_date);
        endDate.setHours(23, 59, 59, 999);
        filteredConversions = filteredConversions.filter(item => 
          new Date(item.timestamp) <= endDate
        );
      }
      
      if (source) {
        filteredConversions = filteredConversions.filter(item => 
          item.source === source
        );
      }
      
      if (campaign) {
        filteredConversions = filteredConversions.filter(item => 
          item.campaign === campaign
        );
      }
      
      // Calculate analytics
      const totalConversions = filteredConversions.length;
      const totalRevenue = filteredConversions.reduce((sum, item) => 
        sum + (parseFloat(item.order_total) || 0), 0
      );
      const avgOrderValue = totalConversions > 0 ? totalRevenue / totalConversions : 0;
      
      // Top sources
      const sourceCounts = {};
      const campaignCounts = {};
      const landingPageCounts = {};
      
      filteredConversions.forEach(item => {
        sourceCounts[item.source] = (sourceCounts[item.source] || 0) + 1;
        campaignCounts[item.campaign] = (campaignCounts[item.campaign] || 0) + 1;
        if (item.landing_page) {
          landingPageCounts[item.landing_page] = (landingPageCounts[item.landing_page] || 0) + 1;
        }
      });
      
      // Sort by count (descending)
      const topSources = Object.entries(sourceCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10);
      
      const topCampaigns = Object.entries(campaignCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10);
        
      const topLandingPages = Object.entries(landingPageCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10);
      
      // Daily conversion trends
      const dailyCounts = {};
      filteredConversions.forEach(item => {
        const date = new Date(item.timestamp).toISOString().split('T')[0];
        dailyCounts[date] = (dailyCounts[date] || 0) + 1;
      });
      
      const analytics = {
        summary: {
          total_conversions: totalConversions,
          total_revenue: totalRevenue,
          avg_order_value: avgOrderValue,
          date_range: {
            start: start_date,
            end: end_date
          }
        },
        top_sources: topSources.map(([source, count]) => ({
          source,
          conversions: count,
          percentage: ((count / totalConversions) * 100).toFixed(1)
        })),
        top_campaigns: topCampaigns.map(([campaign, count]) => ({
          campaign,
          conversions: count,
          percentage: ((count / totalConversions) * 100).toFixed(1)
        })),
        top_landing_pages: topLandingPages.map(([page, count]) => ({
          landing_page: page,
          conversions: count,
          percentage: ((count / totalConversions) * 100).toFixed(1)
        })),
        daily_trends: Object.entries(dailyCounts)
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([date, count]) => ({ date, conversions: count })),
        conversions: filteredConversions.map(item => ({
          timestamp: item.timestamp,
          source: item.source,
          campaign: item.campaign,
          content: item.content,
          landing_page: item.landing_page,
          email: item.email,
          name: item.name,
          order_total: item.order_total,
          offer_name: item.offer_name,
          order_id: item.order_id,
          payment_gateway: item.payment_gateway,
          attribution_found: item.attribution_found
        }))
      };
      
      console.log(`📊 Analytics query returned ${totalConversions} conversions`);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify(analytics)
      };
      
    } catch (error) {
      console.error('❌ Analytics error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'POST') {
    // Store conversion data (called from your track function)
    try {
      const conversionData = JSON.parse(event.body);
      
      // Store with timestamp as key for easy retrieval
      const key = `conversion:${conversionData.timestamp}:${Math.random()}`;
      conversionStore.set(key, conversionData);
      
      // Keep only last 1000 conversions to manage memory
      if (conversionStore.size > 1000) {
        const entries = Array.from(conversionStore.entries())
          .sort(([,a], [,b]) => new Date(b.timestamp) - new Date(a.timestamp))
          .slice(0, 800);
        conversionStore.clear();
        entries.forEach(([key, value]) => conversionStore.set(key, value));
      }
      
      console.log(`📊 Stored conversion for analytics: ${conversionData.email}`);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ success: true })
      };
      
    } catch (error) {
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

module.exports = { handler };
