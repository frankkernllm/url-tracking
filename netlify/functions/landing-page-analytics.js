// netlify/functions/landing-page-analytics.js

const getLandingPageAnalytics = async (redis, options) => {
  const {
    startDate,
    endDate,
    granularity = 'daily', // 'hourly' or 'daily'
    landingPage = null,    // Filter specific landing page
    limit = 100,
    sortBy = 'unique_pageviews', // 'unique_pageviews', 'percentage', 'landing_page'
    sortOrder = 'desc'     // 'asc' or 'desc'
  } = options;

  if (granularity === 'hourly') {
    return await getHourlyAnalytics(redis, startDate, endDate, landingPage, limit, sortBy, sortOrder);
  } else {
    return await getDailyAnalytics(redis, startDate, endDate, landingPage, limit, sortBy, sortOrder);
  }
};

const getHourlyAnalytics = async (redis, startDate, endDate, landingPageFilter, limit, sortBy, sortOrder) => {
  const start = new Date(startDate);
  const end = new Date(endDate);
  const hourlyKeys = [];
  
  // Generate hourly keys for date range
  for (let d = new Date(start); d <= end; d.setHours(d.getHours() + 1)) {
    const hourKey = d.toISOString().substring(0, 13).replace('T', '-');
    hourlyKeys.push(`landing_page_hourly:${hourKey}`);
  }
  
  console.log(`Querying ${hourlyKeys.length} hourly indexes from ${startDate} to ${endDate}`);
  
  const hourlyData = await redis.mget(hourlyKeys);
  const results = [];
  
  hourlyData.forEach((data, index) => {
    if (data) {
      try {
        const parsed = JSON.parse(data);
        
        if (landingPageFilter) {
          // Filter for specific landing page
          const pageData = parsed.landing_pages[landingPageFilter];
          if (pageData) {
            results.push({
              hour: parsed.hour,
              landing_page: landingPageFilter,
              unique_pageviews: pageData.count,
              percentage: parseFloat(pageData.percentage),
              sources: pageData.sources,
              total_unique_ips_in_hour: parsed.total_unique_ips
            });
          }
        } else {
          // Return all landing pages for this hour
          Object.entries(parsed.landing_pages).forEach(([page, pageData]) => {
            results.push({
              hour: parsed.hour,
              landing_page: page,
              unique_pageviews: pageData.count,
              percentage: parseFloat(pageData.percentage),
              sources: pageData.sources,
              total_unique_ips_in_hour: parsed.total_unique_ips
            });
          });
        }
      } catch (parseError) {
        console.error(`Error parsing hourly data:`, parseError);
      }
    }
  });
  
  // Sort results
  results.sort((a, b) => {
    const aVal = sortBy === 'landing_page' ? a[sortBy] : parseFloat(a[sortBy]) || 0;
    const bVal = sortBy === 'landing_page' ? b[sortBy] : parseFloat(b[sortBy]) || 0;
    
    if (sortOrder === 'desc') {
      return typeof aVal === 'string' ? bVal.localeCompare(aVal) : bVal - aVal;
    } else {
      return typeof aVal === 'string' ? aVal.localeCompare(bVal) : aVal - bVal;
    }
  });
  
  return {
    granularity: 'hourly',
    date_range: { start: startDate, end: endDate },
    landing_page_filter: landingPageFilter,
    total_records: results.length,
    data: results.slice(0, limit)
  };
};

const getDailyAnalytics = async (redis, startDate, endDate, landingPageFilter, limit, sortBy, sortOrder) => {
  // Get hourly data first
  const hourlyResults = await getHourlyAnalytics(redis, startDate, endDate, landingPageFilter, 99999, 'hour', 'asc');
  
  // Aggregate by day and landing page
  const dailyAggregations = {};
  
  hourlyResults.data.forEach(hourData => {
    const date = hourData.hour.substring(0, 10); // Extract YYYY-MM-DD
    const page = hourData.landing_page;
    
    if (!dailyAggregations[date]) {
      dailyAggregations[date] = {};
    }
    
    if (!dailyAggregations[date][page]) {
      dailyAggregations[date][page] = {
        unique_pageviews: 0,
        total_hours_with_data: 0,
        sources: {},
        hourly_breakdown: []
      };
    }
    
    // Aggregate the data
    dailyAggregations[date][page].unique_pageviews += hourData.unique_pageviews;
    dailyAggregations[date][page].total_hours_with_data += 1;
    dailyAggregations[date][page].hourly_breakdown.push({
      hour: hourData.hour.substring(11), // Just the hour part
      unique_pageviews: hourData.unique_pageviews
    });
    
    // Merge sources
    Object.entries(hourData.sources || {}).forEach(([source, count]) => {
      dailyAggregations[date][page].sources[source] = 
        (dailyAggregations[date][page].sources[source] || 0) + count;
    });
  });
  
  // Convert to flat array for sorting
  const results = [];
  Object.entries(dailyAggregations).forEach(([date, pages]) => {
    Object.entries(pages).forEach(([page, data]) => {
      results.push({
        date,
        landing_page: page,
        unique_pageviews: data.unique_pageviews,
        hours_with_data: data.total_hours_with_data,
        sources: data.sources,
        hourly_breakdown: data.hourly_breakdown,
        top_source: Object.entries(data.sources).sort(([,a], [,b]) => b - a)[0]?.[0] || 'unknown'
      });
    });
  });
  
  // Sort results
  results.sort((a, b) => {
    const aVal = sortBy === 'landing_page' || sortBy === 'date' ? a[sortBy] : parseFloat(a[sortBy]) || 0;
    const bVal = sortBy === 'landing_page' || sortBy === 'date' ? b[sortBy] : parseFloat(b[sortBy]) || 0;
    
    if (sortOrder === 'desc') {
      return typeof aVal === 'string' ? bVal.localeCompare(aVal) : bVal - aVal;
    } else {
      return typeof aVal === 'string' ? aVal.localeCompare(bVal) : aVal - bVal;
    }
  });
  
  return {
    granularity: 'daily',
    date_range: { start: startDate, end: endDate },
    landing_page_filter: landingPageFilter,
    total_records: results.length,
    data: results.slice(0, limit)
  };
};

const getTopLandingPages = async (redis, startDate, endDate, limit = 10) => {
  const dailyResults = await getDailyAnalytics(redis, startDate, endDate, null, 99999, 'unique_pageviews', 'desc');
  
  // Aggregate by landing page across all days
  const pageAggregations = {};
  
  dailyResults.data.forEach(dayData => {
    const page = dayData.landing_page;
    
    if (!pageAggregations[page]) {
      pageAggregations[page] = {
        landing_page: page,
        total_unique_pageviews: 0,
        days_with_data: 0,
        total_sources: {},
        avg_daily_pageviews: 0
      };
    }
    
    pageAggregations[page].total_unique_pageviews += dayData.unique_pageviews;
    pageAggregations[page].days_with_data += 1;
    
    // Merge sources
    Object.entries(dayData.sources || {}).forEach(([source, count]) => {
      pageAggregations[page].total_sources[source] = 
        (pageAggregations[page].total_sources[source] || 0) + count;
    });
  });
  
  // Calculate averages and convert to array
  const topPages = Object.values(pageAggregations).map(page => ({
    ...page,
    avg_daily_pageviews: (page.total_unique_pageviews / page.days_with_data).toFixed(1),
    top_source: Object.entries(page.total_sources).sort(([,a], [,b]) => b - a)[0]?.[0] || 'unknown'
  }));
  
  // Sort by total unique pageviews
  topPages.sort((a, b) => b.total_unique_pageviews - a.total_unique_pageviews);
  
  return {
    date_range: { start: startDate, end: endDate },
    total_pages: topPages.length,
    data: topPages.slice(0, limit)
  };
};

exports.handler = async (event, context) => {
  // API Key authentication
  const apiKey = event.headers['x-api-key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  // Redis client setup
  const redis = {
    async get(key) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/get/${encodeURIComponent(key)}`, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result;
    },
    
    async mget(keys) {
      if (keys.length === 0) return [];
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/mget/${keys.map(k => encodeURIComponent(k)).join('/')}`, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      });
      const data = await response.json();
      return data.result || [];
    }
  };

  const { 
    start_date = '2025-07-19',
    end_date = '2025-07-19',
    granularity = 'daily',
    landing_page = null,
    limit = 100,
    sort_by = 'unique_pageviews',
    sort_order = 'desc',
    action = 'analytics' // 'analytics' or 'top_pages'
  } = event.queryStringParameters || {};

  try {
    console.log(`Landing page analytics query: ${action} from ${start_date} to ${end_date}`);
    
    let analytics;
    
    if (action === 'top_pages') {
      analytics = await getTopLandingPages(redis, start_date, end_date, parseInt(limit));
    } else {
      analytics = await getLandingPageAnalytics(redis, {
        startDate: start_date,
        endDate: end_date,
        granularity,
        landingPage: landing_page,
        limit: parseInt(limit),
        sortBy: sort_by,
        sortOrder: sort_order
      });
    }

    return {
      statusCode: 200,
      headers: { 
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      },
      body: JSON.stringify({
        success: true,
        query_parameters: {
          start_date,
          end_date,
          granularity,
          landing_page,
          limit: parseInt(limit),
          sort_by,
          sort_order,
          action
        },
        execution_time_ms: new Date().getTime(),
        ...analytics
      })
    };
  } catch (error) {
    console.error('Landing page analytics error:', error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({
        success: false,
        error: 'Analytics query failed',
        message: error.message
      })
    };
  }
};
