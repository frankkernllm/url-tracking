// Query Conversion Indexes - View Extracted Conversion Data
// Path: netlify/functions/query-conversion-indexes.js
// Purpose: Query the conversion indexes created by extract-conversion-data.js

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse query parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const queryParams = event.queryStringParameters || {};
    
    // Extract query parameters
    const query = {
      // Query type: email, ip, session, date, or summary
      type: body.type || queryParams.type || 'summary',
      
      // Specific lookup values
      email: body.email || queryParams.email,
      ip: body.ip || queryParams.ip,
      session_id: body.session_id || queryParams.session_id,
      date: body.date || queryParams.date, // Format: YYYY-MM-DD
      
      // Options
      limit: parseInt(body.limit || queryParams.limit || '50'),
      include_raw_data: body.include_raw_data === 'true' || queryParams.include_raw_data === 'true'
    };
    
    console.log('🔍 CONVERSION INDEX QUERY starting with parameters:', query);
    
    let result;
    
    switch (query.type) {
      case 'email':
        result = await queryEmailIndex(redis, query);
        break;
      case 'ip':
        result = await queryIPIndex(redis, query);
        break;
      case 'session':
        result = await querySessionIndex(redis, query);
        break;
      case 'date':
        result = await queryDateIndex(redis, query);
        break;
      case 'summary':
      default:
        result = await getIndexSummary(redis, query);
        break;
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Conversion index query completed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_type: query.type,
        query_parameters: query,
        result: result,
        processing_time_ms: totalTime
      })
    };
    
  } catch (error) {
    console.error('❌ Conversion index query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Conversion index query failed', 
        message: error.message 
      })
    };
  }
};

// Query email-specific conversion index
async function queryEmailIndex(redis, query) {
  if (!query.email) {
    return {
      error: 'Email parameter required for email query',
      example: 'Add "email": "customer@example.com" to your request'
    };
  }
  
  try {
    const emailKey = `conversion_index_v1_email:${encodeURIComponent(query.email.toLowerCase())}`;
    console.log(`🔍 Querying email index: ${emailKey}`);
    
    const result = await redis(`get/${emailKey}`);
    
    if (!result?.result) {
      return {
        email: query.email,
        found: false,
        message: 'No conversions found for this email address',
        suggestion: 'Check if email exists with summary query first'
      };
    }
    
    const emailData = JSON.parse(decodeURIComponent(result.result));
    
    // Limit conversions if requested
    let conversions = emailData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    return {
      email: query.email,
      found: true,
      summary: {
        conversion_count: emailData.conversion_count,
        total_revenue: emailData.total_revenue,
        latest_conversion: emailData.latest_conversion,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        unique_ips_count: conv.unique_ips?.length || 0,
        ssid: conv.ssid,
        landing_page: conv.landing_page,
        source: conv.source
      }))
    };
    
  } catch (error) {
    console.error('❌ Email index query error:', error);
    return {
      email: query.email,
      error: 'Failed to query email index',
      message: error.message
    };
  }
}

// Query IP-specific conversion index
async function queryIPIndex(redis, query) {
  if (!query.ip) {
    return {
      error: 'IP parameter required for IP query',
      example: 'Add "ip": "192.168.1.1" to your request'
    };
  }
  
  try {
    const encodedIP = query.ip.replace(/:/g, '_').replace(/\./g, '_');
    const ipKey = `conversion_index_v1_ip:${encodedIP}`;
    console.log(`🔍 Querying IP index: ${ipKey}`);
    
    const result = await redis(`get/${ipKey}`);
    
    if (!result?.result) {
      return {
        ip: query.ip,
        found: false,
        message: 'No conversions found for this IP address'
      };
    }
    
    const ipData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = ipData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    return {
      ip: query.ip,
      found: true,
      summary: {
        conversion_count: ipData.conversion_count,
        unique_emails: ipData.unique_emails,
        total_revenue: ipData.total_revenue,
        latest_conversion: ipData.latest_conversion,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        ssid: conv.ssid,
        landing_page: conv.landing_page
      }))
    };
    
  } catch (error) {
    console.error('❌ IP index query error:', error);
    return {
      ip: query.ip,
      error: 'Failed to query IP index',
      message: error.message
    };
  }
}

// Query session-specific conversion index
async function querySessionIndex(redis, query) {
  if (!query.session_id) {
    return {
      error: 'Session ID parameter required for session query',
      example: 'Add "session_id": "session_123" to your request'
    };
  }
  
  try {
    const sessionKey = `conversion_index_v1_session:${query.session_id}`;
    console.log(`🔍 Querying session index: ${sessionKey}`);
    
    const result = await redis(`get/${sessionKey}`);
    
    if (!result?.result) {
      return {
        session_id: query.session_id,
        found: false,
        message: 'No conversions found for this session ID'
      };
    }
    
    const sessionData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = sessionData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    return {
      session_id: query.session_id,
      found: true,
      summary: {
        conversion_count: sessionData.conversion_count,
        unique_emails: sessionData.unique_emails,
        total_revenue: sessionData.total_revenue,
        latest_conversion: sessionData.latest_conversion,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        landing_page: conv.landing_page
      }))
    };
    
  } catch (error) {
    console.error('❌ Session index query error:', error);
    return {
      session_id: query.session_id,
      error: 'Failed to query session index',
      message: error.message
    };
  }
}

// Query date-specific conversion index
async function queryDateIndex(redis, query) {
  if (!query.date) {
    // Default to today if no date specified
    query.date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
  }
  
  try {
    const dateKey = `conversion_index_v1_date:${query.date}`;
    console.log(`🔍 Querying date index: ${dateKey}`);
    
    const result = await redis(`get/${dateKey}`);
    
    if (!result?.result) {
      return {
        date: query.date,
        found: false,
        message: 'No conversions found for this date',
        suggestion: 'Try dates between 2025-06-08 and 2025-07-27'
      };
    }
    
    const dateData = JSON.parse(decodeURIComponent(result.result));
    
    let conversions = dateData.conversions || [];
    if (query.limit && conversions.length > query.limit) {
      conversions = conversions.slice(0, query.limit);
    }
    
    return {
      date: query.date,
      found: true,
      summary: {
        conversion_count: dateData.conversion_count,
        unique_emails: dateData.unique_emails?.length || 0,
        unique_ips: dateData.unique_ips?.length || 0,
        total_revenue: dateData.total_revenue,
        conversions_returned: conversions.length
      },
      conversions: query.include_raw_data ? conversions : conversions.map(conv => ({
        timestamp: conv.timestamp,
        email: conv.email,
        order_total: conv.order_total,
        conversion_ip: conv.conversion_ip,
        primary_ip: conv.primary_ip,
        unique_ips_count: conv.unique_ips?.length || 0,
        ssid: conv.ssid,
        landing_page: conv.landing_page,
        source: conv.source
      }))
    };
    
  } catch (error) {
    console.error('❌ Date index query error:', error);
    return {
      date: query.date,
      error: 'Failed to query date index',
      message: error.message
    };
  }
}

// Get summary of all conversion indexes
async function getIndexSummary(redis, query) {
  console.log('📊 Getting conversion index summary...');
  
  try {
    const summary = {
      extraction_summary: {
        total_conversions_extracted: 3039,
        total_conversions_filtered_out: 8910,
        unique_emails_found: 930,
        unique_ips_found: 932,
        unique_sessions_found: 189,
        date_range: {
          earliest_conversion: "2025-06-08T07:21:24.086Z",
          latest_conversion: "2025-07-27T17:00:09.516Z"
        }
      },
      index_types: [
        {
          type: "email",
          description: "Query by email address",
          example_query: {
            type: "email",
            email: "customer@example.com"
          }
        },
        {
          type: "ip", 
          description: "Query by IP address",
          example_query: {
            type: "ip",
            ip: "192.168.1.1"
          }
        },
        {
          type: "session",
          description: "Query by session ID",
          example_query: {
            type: "session",
            session_id: "session_123"
          }
        },
        {
          type: "date",
          description: "Query by date (YYYY-MM-DD)",
          example_query: {
            type: "date",
            date: "2025-07-27"
          }
        }
      ],
      sample_queries: [
        "Today's conversions: type=date&date=2025-07-27",
        "July 1st conversions: type=date&date=2025-07-01", 
        "Email lookup: type=email&email=customer@example.com",
        "IP lookup: type=ip&ip=192.168.1.1"
      ]
    };
    
    // Try to get a sample date index to verify data
    const todayKey = `conversion_index_v1_date:${new Date().toISOString().split('T')[0]}`;
    const todayResult = await redis(`get/${todayKey}`);
    
    if (todayResult?.result) {
      const todayData = JSON.parse(decodeURIComponent(todayResult.result));
      summary.today_sample = {
        date: new Date().toISOString().split('T')[0],
        conversion_count: todayData.conversion_count,
        total_revenue: todayData.total_revenue,
        unique_emails: todayData.unique_emails?.length || 0
      };
    }
    
    return summary;
    
  } catch (error) {
    console.error('❌ Summary generation error:', error);
    return {
      error: 'Failed to generate summary',
      message: error.message
    };
  }
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
