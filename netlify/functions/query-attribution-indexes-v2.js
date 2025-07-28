// Query V2 Attribution Indexes - Debug V2 Attribution Data
// Path: netlify/functions/query-attribution-indexes-v2.js
// Purpose: Query the V2 attribution indexes created by build-attribution-indexes-v2.js

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
      // Query type: session, ip, landing, source, or summary
      type: body.type || queryParams.type || 'summary',
      
      // Specific lookup values
      session_id: body.session_id || queryParams.session_id,
      ip: body.ip || queryParams.ip,
      landing_page: body.landing_page || queryParams.landing_page,
      source: body.source || queryParams.source,
      
      // Options
      limit: parseInt(body.limit || queryParams.limit || '10'),
      show_pageview_details: body.show_pageview_details === 'true' || queryParams.show_pageview_details === 'true'
    };
    
    console.log('🔍 V2 ATTRIBUTION INDEX QUERY starting with parameters:', query);
    
    let result;
    
    switch (query.type) {
      case 'session':
        result = await queryV2SessionIndex(redis, query);
        break;
      case 'ip':
        result = await queryV2IPIndex(redis, query);
        break;
      case 'landing':
        result = await queryV2LandingIndex(redis, query);
        break;
      case 'source':
        result = await queryV2SourceIndex(redis, query);
        break;
      case 'summary':
      default:
        result = await getV2AttributionSummary(redis, query);
        break;
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ V2 Attribution index query completed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_version: 'v2_attribution',
        query_type: query.type,
        query_parameters: query,
        result: result,
        processing_time_ms: totalTime
      })
    };
    
  } catch (error) {
    console.error('❌ V2 Attribution index query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'V2 Attribution index query failed', 
        message: error.message 
      })
    };
  }
};

// Query V2 session attribution index
async function queryV2SessionIndex(redis, query) {
  if (!query.session_id) {
    return {
      error: 'Session ID parameter required for session query',
      example: 'Add "session_id": "1753646689544-bbahy0dx9" to your request'
    };
  }
  
  try {
    const sessionKey = `attribution_index_v2_session:${query.session_id}`;
    console.log(`🔍 Querying V2 session index: ${sessionKey}`);
    
    const result = await redis(`get/${sessionKey}`);
    
    if (!result?.result) {
      return {
        session_id: query.session_id,
        found: false,
        message: 'No V2 attribution index found for this session ID'
      };
    }
    
    const sessionData = JSON.parse(decodeURIComponent(result.result));
    
    // Limit pageviews if requested
    let pageviews = sessionData.pageviews || [];
    const originalCount = pageviews.length;
    
    if (query.limit && pageviews.length > query.limit) {
      pageviews = pageviews.slice(0, query.limit);
    }
    
    const response = {
      session_id: query.session_id,
      found: true,
      summary: {
        pageview_count: sessionData.pageview_count || originalCount,
        latest_timestamp: sessionData.latest_timestamp,
        earliest_timestamp: sessionData.earliest_timestamp,
        unique_ips: sessionData.ip_addresses?.length || 0,
        unique_landing_pages: sessionData.landing_pages?.length || 0,
        unique_sources: sessionData.sources?.length || 0,
        pageviews_returned: pageviews.length
      },
      v2_metadata: {
        index_type: sessionData.index_type,
        created_at: sessionData.created_at,
        data_sources: sessionData.data_sources,
        version: sessionData.version
      }
    };
    
    // Add pageview details if requested
    if (query.show_pageview_details) {
      response.pageviews = pageviews.map(pv => ({
        timestamp: pv.timestamp,
        ip_address: pv.ip_address,
        landing_page: pv.landing_page,
        source: pv.source,
        attribution_method: pv.attribution_method,
        is_verification_data: pv.is_verification_data
      }));
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Session index query error:', error);
    return {
      session_id: query.session_id,
      error: 'Failed to query V2 session index',
      message: error.message
    };
  }
}

// Query V2 IP attribution index  
async function queryV2IPIndex(redis, query) {
  if (!query.ip) {
    return {
      error: 'IP parameter required for IP query',
      example: 'Add "ip": "24.165.221.165" to your request'
    };
  }
  
  try {
    const encodedIP = query.ip.replace(/:/g, '_').replace(/\./g, '_');
    const ipKey = `attribution_index_v2_ip:${encodedIP}`;
    console.log(`🔍 Querying V2 IP index: ${ipKey}`);
    
    const result = await redis(`get/${ipKey}`);
    
    if (!result?.result) {
      return {
        ip: query.ip,
        found: false,
        message: 'No V2 attribution index found for this IP address'
      };
    }
    
    const ipData = JSON.parse(decodeURIComponent(result.result));
    
    // Limit pageviews if requested
    let pageviews = ipData.pageviews || [];
    const originalCount = pageviews.length;
    
    if (query.limit && pageviews.length > query.limit) {
      pageviews = pageviews.slice(0, query.limit);
    }
    
    const response = {
      ip: query.ip,
      found: true,
      summary: {
        pageview_count: ipData.pageview_count || originalCount,
        latest_timestamp: ipData.latest_timestamp,
        earliest_timestamp: ipData.earliest_timestamp,
        unique_sessions: ipData.session_ids?.length || 0,
        unique_landing_pages: ipData.landing_pages?.length || 0,
        unique_sources: ipData.sources?.length || 0,
        pageviews_returned: pageviews.length,
        potential_cross_user_data: originalCount > 20 // Flag if likely multiple users
      },
      v2_metadata: {
        index_type: ipData.index_type,
        created_at: ipData.created_at,
        data_sources: ipData.data_sources,
        version: ipData.version
      }
    };
    
    // Add pageview details if requested
    if (query.show_pageview_details) {
      response.pageviews = pageviews.map(pv => ({
        timestamp: pv.timestamp,
        session_id: pv.session_id,
        landing_page: pv.landing_page,
        source: pv.source,
        attribution_method: pv.attribution_method,
        is_verification_data: pv.is_verification_data
      }));
      
      // Add unique session analysis
      const uniqueSessions = [...new Set(pageviews.map(pv => pv.session_id).filter(Boolean))];
      response.session_analysis = {
        unique_sessions_in_sample: uniqueSessions.length,
        sessions: uniqueSessions.slice(0, 5), // Show first 5 sessions
        likely_multiple_users: uniqueSessions.length > 3
      };
    }
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 IP index query error:', error);
    return {
      ip: query.ip,
      error: 'Failed to query V2 IP index',
      message: error.message
    };
  }
}

// Query V2 landing page attribution index
async function queryV2LandingIndex(redis, query) {
  if (!query.landing_page) {
    return {
      error: 'Landing page parameter required for landing page query',
      example: 'Add "landing_page": "https://ojoy.ai/" to your request'
    };
  }
  
  try {
    const encodedLP = encodeURIComponent(query.landing_page).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 100);
    const lpKey = `attribution_index_v2_landing:${encodedLP}`;
    console.log(`🔍 Querying V2 landing page index: ${lpKey}`);
    
    const result = await redis(`get/${lpKey}`);
    
    if (!result?.result) {
      return {
        landing_page: query.landing_page,
        found: false,
        message: 'No V2 attribution index found for this landing page'
      };
    }
    
    const lpData = JSON.parse(decodeURIComponent(result.result));
    
    const response = {
      landing_page: query.landing_page,
      found: true,
      summary: {
        pageview_count: lpData.pageview_count,
        latest_timestamp: lpData.latest_timestamp,
        unique_ips: lpData.ip_addresses?.length || 0,
        unique_sessions: lpData.session_ids?.length || 0,
        unique_sources: lpData.sources?.length || 0,
        cross_user_warning: lpData.pageview_count > 100 // Warn if likely cross-user data
      },
      v2_metadata: {
        index_type: lpData.index_type,
        created_at: lpData.created_at,
        data_sources: lpData.data_sources
      }
    };
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Landing page index query error:', error);
    return {
      landing_page: query.landing_page,
      error: 'Failed to query V2 landing page index',
      message: error.message
    };
  }
}

// Query V2 source attribution index
async function queryV2SourceIndex(redis, query) {
  if (!query.source) {
    return {
      error: 'Source parameter required for source query',
      example: 'Add "source": "direct_typed" to your request'
    };
  }
  
  try {
    const encodedSource = encodeURIComponent(query.source).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
    const sourceKey = `attribution_index_v2_source:${encodedSource}`;
    console.log(`🔍 Querying V2 source index: ${sourceKey}`);
    
    const result = await redis(`get/${sourceKey}`);
    
    if (!result?.result) {
      return {
        source: query.source,
        found: false,
        message: 'No V2 attribution index found for this source'
      };
    }
    
    const sourceData = JSON.parse(decodeURIComponent(result.result));
    
    const response = {
      source: query.source,
      found: true,
      summary: {
        pageview_count: sourceData.pageview_count,
        latest_timestamp: sourceData.latest_timestamp,
        unique_ips: sourceData.ip_addresses?.length || 0,
        unique_sessions: sourceData.session_ids?.length || 0,
        unique_landing_pages: sourceData.landing_pages?.length || 0,
        cross_user_warning: sourceData.pageview_count > 100 // Warn if likely cross-user data
      },
      v2_metadata: {
        index_type: sourceData.index_type,
        created_at: sourceData.created_at,
        data_sources: sourceData.data_sources
      }
    };
    
    return response;
    
  } catch (error) {
    console.error('❌ V2 Source index query error:', error);
    return {
      source: query.source,
      error: 'Failed to query V2 source index',
      message: error.message
    };
  }
}

// Get summary of V2 attribution indexes
async function getV2AttributionSummary(redis, query) {
  console.log('📊 Getting V2 attribution index summary...');
  
  return {
    v2_attribution_summary: {
      purpose: 'Debug V2 attribution indexes to identify cross-user data contamination',
      available_queries: [
        {
          type: 'session',
          description: 'Query session-specific pageviews (should be single user)',
          example: { type: 'session', session_id: '1753646689544-bbahy0dx9' }
        },
        {
          type: 'ip',
          description: 'Query IP-specific pageviews (may contain multiple users)',
          example: { type: 'ip', ip: '24.165.221.165', show_pageview_details: true }
        },
        {
          type: 'landing',
          description: 'Query landing page pageviews (likely contains many users)',
          example: { type: 'landing', landing_page: 'https://ojoy.ai/' }
        },
        {
          type: 'source',
          description: 'Query source-specific pageviews (likely contains many users)',
          example: { type: 'source', source: 'direct_typed' }
        }
      ]
    },
    debugging_tips: {
      massive_data_problem: 'If V2 attribution returns too many touchpoints, check:',
      steps: [
        '1. Query the session index - should have reasonable pageview count',
        '2. Query the IP indexes - may show multiple sessions (multiple users)',
        '3. Landing page and source indexes likely contain hundreds/thousands of pageviews',
        '4. Use show_pageview_details: true to see session_id distribution'
      ],
      cross_user_indicators: [
        'pageview_count > 50 for IP indexes',
        'unique_sessions > 5 for IP indexes',
        'pageview_count > 100 for landing/source indexes'
      ]
    },
    recommended_tests: [
      { type: 'session', session_id: '1753646689544-bbahy0dx9', show_pageview_details: true },
      { type: 'ip', ip: '24.165.221.165', show_pageview_details: true },
      { type: 'ip', ip: '2603:6013:b800:3c7e:19af:1695:3084:a76f', show_pageview_details: true }
    ]
  };
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
