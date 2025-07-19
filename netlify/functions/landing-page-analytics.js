// netlify/functions/landing-page-analytics.js

const debugRedisKeys = async (redis, date) => {
  console.log(`Debug: Checking Redis keys for date ${date}`);
  
  // Check pageview indexes that should contain the source data
  const allPageviewKeys = await redis.keys('pageview_index_ip:*');
  console.log(`Found ${allPageviewKeys.length} total pageview_index_ip keys`);
  
  // Check landing page hourly keys
  const allLandingPageKeys = await redis.keys('landing_page_hourly:*');
  console.log(`Found ${allLandingPageKeys.length} total landing_page_hourly keys`);
  
  // Sample a few pageview indexes to see their structure and timestamps
  const samplePageviewKeys = allPageviewKeys.slice(0, 5);
  const samplePageviewData = [];
  
  for (const key of samplePageviewKeys) {
    const data = await redis.get(key);
    if (data) {
      const parsed = JSON.parse(data);
      samplePageviewData.push({
        key: key,
        timestamp: parsed.timestamp,
        landing_page: parsed.landing_page,
        ip_address: parsed.ip_address,
        source: parsed.source
      });
    }
  }
  
  // Check a specific hour that should have data
  const hourKey = `landing_page_hourly:${date}-11`; // 11 AM
  const hourData = await redis.get(hourKey);
  
  return {
    total_pageview_index_keys: allPageviewKeys.length,
    total_landing_page_keys: allLandingPageKeys.length,
    sample_pageview_data: samplePageviewData,
    test_hour_key: hourKey,
    test_hour_exists: !!hourData,
    test_hour_data: hourData ? JSON.parse(hourData) : null,
    issue_diagnosis: allPageviewKeys.length === 0 ? 
      "NO PAGEVIEW INDEXES FOUND - Run build-indexes-complete first" : 
      "Pageview indexes exist - check timestamp format mismatch"
  };
};

const getLandingPageAnalytics = async (redis, options) => {
  const {
    startDate,
    endDate,
    granularity = 'daily',
    landingPage = null,
    limit = 100,
    sortBy = 'unique_pageviews',
    sortOrder = 'desc'
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
  let keysWithData = 0;
  let keysWithLandingPages = 0;
  
  hourlyData.forEach((data, index) => {
    if (data) {
      keysWithData++;
      try {
        const parsed = JSON.parse(data);
        
        // Count keys that have actual landing page data
        if (parsed.landing_pages && Object.keys(parsed.landing_pages).length > 0) {
          keysWithLandingPages++;
          
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
        }
      } catch (parseError) {
        console.error(`Error parsing hourly data for key ${hourlyKeys[index]}:`, parseError);
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
    keys_queried: hourlyKeys.length,
    keys_with_data: keysWithData,
    keys_with_landing_pages: keysWithLandingPages,
    data: results.slice(0, limit)
  };
};

const getDailyAnalytics = async (redis, startDate, endDate, landingPageFilter, limit, sortBy, sortOrder) => {
  // Get hourly data first
  const hourlyResults = await getHourlyAnalytics(redis, startDate, endDate, landingPageFilter, 99999, 'hour', 'asc');
  
  if (hourlyResults.total_records === 0) {
    return {
      granularity: 'daily',
      date_range: { start: startDate, end: endDate },
      landing_page_filter: landingPageFilter,
      total_records: 0,
      keys_queried: hourlyResults.keys_queried,
      keys_with_data: hourlyResults.keys_with_data,
      keys_with_landing_pages: hourlyResults.keys_with_landing_pages,
      diagnosis: hourlyResults.keys_with_data > 0 && hourlyResults.keys_with_landing_pages === 0 ? 
        "Hours processed but no landing page data found - check pageview_index_ip data" : 
        "No hourly data found",
      data: []
    };
  }
  
  // Aggregate by day and landing page
  const dailyAggregations = {};
  
  hourlyResults.data.forEach(hourData => {
    const date = hourData.hour.substring(0, 10);
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
    
    dailyAggregations[date][page].unique_pageviews += hourData.unique_pageviews;
    dailyAggregations[date][page].total_hours_with_data += 1;
    dailyAggregations[date][page].hourly_breakdown.push({
      hour: hourData.hour.substring(11),
      unique_pageviews: hourData.unique_pageviews
    });
    
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
    keys_queried: hourlyResults.keys_queried,
    keys_with_data: hourlyResults.keys_with_data,
    keys_with_landing_pages: hourlyResults.keys_with_landing_pages,
    data: results.slice(0, limit)
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
    },
    
    async keys(pattern) {
      const response = await fetch(`${process.env.UPSTASH_REDIS_REST_URL}/keys/${encodeURIComponent(pattern)}`, {
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
    action = 'analytics'
  } = event.queryStringParameters || {};

  // Debug mode
  if (event.queryStringParameters?.debug === 'true') {
    try {
      const debugData = await debugRedisKeys(redis, start_date);
      return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          debug: true,
          date_requested: start_date,
          debug_results: debugData
        })
      };
    } catch (debugError) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          debug: true,
          error: debugError.message
        })
      };
    }
  }

  try {
    console.log(`Landing page analytics query: ${action} from ${start_date} to ${end_date}`);
    
    const analytics = await getLandingPageAnalytics(redis, {
      startDate: start_date,
      endDate: end_date,
      granularity,
      landingPage: landing_page,
      limit: parseInt(limit),
      sortBy: sort_by,
      sortOrder: sort_order
    });

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
