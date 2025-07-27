// Query Attribution Indexes - Find Pageviews by IP, Session, Landing Page, etc.
// Path: netlify/functions/query-attribution-indexes.js
// Purpose: Query attribution indexes to find pageviews and customer journeys

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse request body
    const requestData = JSON.parse(event.body || '{}');
    const { 
      query_type,    // 'ip', 'session', 'landing_page', 'source'
      ip_address,    // '42.61.210.120'
      session_id,    // 'session_12345'
      landing_page,  // '/landing-page-url'
      source,        // 'google'
      limit = 100,
      include_raw_pageviews = true
    } = requestData;
    
    if (!query_type) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing required field: query_type (ip, session, landing_page, or source)' 
        })
      };
    }
    
    console.log(`🔍 Querying attribution indexes: ${query_type}`);
    
    let queryResult;
    
    switch (query_type) {
      case 'ip':
        if (!ip_address) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'IP address parameter required for IP query' })
          };
        }
        queryResult = await queryIPAttributionIndex(redis, ip_address, limit, include_raw_pageviews);
        break;
        
      case 'session':
        if (!session_id) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Session ID parameter required for session query' })
          };
        }
        queryResult = await querySessionAttributionIndex(redis, session_id, limit, include_raw_pageviews);
        break;
        
      case 'landing_page':
        if (!landing_page) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Landing page parameter required for landing page query' })
          };
        }
        queryResult = await queryLandingPageAttributionIndex(redis, landing_page, limit, include_raw_pageviews);
        break;
        
      case 'source':
        if (!source) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ error: 'Source parameter required for source query' })
          };
        }
        queryResult = await querySourceAttributionIndex(redis, source, limit, include_raw_pageviews);
        break;
        
      default:
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({ 
            error: 'Invalid query_type. Use: ip, session, landing_page, or source' 
          })
        };
    }
    
    const processingTime = Date.now() - startTime;
    
    if (!queryResult) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No attribution index found',
          query_type: query_type,
          query_parameters: { ip_address, session_id, landing_page, source },
          processing_time_ms: processingTime
        })
      };
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_type: query_type,
        query_parameters: { ip_address, session_id, landing_page, source, limit, include_raw_pageviews },
        result: queryResult,
        processing_time_ms: processingTime
      })
    };
    
  } catch (error) {
    console.error('❌ Attribution index query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution index query failed', 
        message: error.message 
      })
    };
  }
};

// Query IP-based attribution index
async function queryIPAttributionIndex(redis, ipAddress, limit, includeRawPageviews) {
  try {
    const encodedIP = encodeIPForKey(ipAddress);
    const ipKey = `attribution_index_v1_ip:${encodedIP}`;
    console.log(`🔍 Querying IP attribution index: ${ipKey}`);
    
    const result = await redis(`get/${ipKey}`, 3000);
    
    if (!result?.result) {
      return {
        ip_address: ipAddress,
        found: false,
        message: 'No pageviews found for this IP address in attribution indexes',
        suggestion: 'Check if attribution indexes have been built with build-attribution-indexes.js'
      };
    }
    
    const ipData = JSON.parse(decodeURIComponent(result.result));
    
    // Apply limit to pageviews
    let pageviews = ipData.pageviews || [];
    if (limit && pageviews.length > limit) {
      pageviews = pageviews.slice(0, limit);
    }
    
    return {
      ip_address: ipAddress,
      found: true,
      summary: {
        pageview_count: ipData.pageview_count,
        latest_timestamp: ipData.latest_timestamp,
        earliest_timestamp: ipData.earliest_timestamp,
        unique_sessions: ipData.session_ids?.length || 0,
        unique_landing_pages: ipData.landing_pages?.length || 0,
        unique_sources: ipData.sources?.length || 0,
        pageviews_returned: pageviews.length
      },
      metadata: {
        session_ids: ipData.session_ids || [],
        landing_pages: ipData.landing_pages || [],
        sources: ipData.sources || []
      },
      pageviews: includeRawPageviews ? pageviews : pageviews.map(pv => ({
        timestamp: pv.timestamp,
        landing_page: pv.landing_page,
        source: pv.source,
        utm_campaign: pv.utm_campaign,
        utm_source: pv.utm_source,
        utm_medium: pv.utm_medium,
        session_id: pv.session_id,
        screen_resolution: pv.screen_resolution,
        referrer_url: pv.referrer_url
      }))
    };
    
  } catch (error) {
    console.error('❌ IP attribution index query error:', error);
    return {
      ip_address: ipAddress,
      error: 'Failed to query IP attribution index',
      message: error.message
    };
  }
}

// Query session-based attribution index
async function querySessionAttributionIndex(redis, sessionId, limit, includeRawPageviews) {
  try {
    const sessionKey = `attribution_index_v1_session:${sessionId}`;
    console.log(`🔍 Querying session attribution index: ${sessionKey}`);
    
    const result = await redis(`get/${sessionKey}`, 3000);
    
    if (!result?.result) {
      return {
        session_id: sessionId,
        found: false,
        message: 'No pageviews found for this session ID in attribution indexes'
      };
    }
    
    const sessionData = JSON.parse(decodeURIComponent(result.result));
    
    let pageviews = sessionData.pageviews || [];
    if (limit && pageviews.length > limit) {
      pageviews = pageviews.slice(0, limit);
    }
    
    return {
      session_id: sessionId,
      found: true,
      summary: {
        pageview_count: sessionData.pageview_count,
        latest_timestamp: sessionData.latest_timestamp,
        earliest_timestamp: sessionData.earliest_timestamp,
        unique_ips: sessionData.ip_addresses?.length || 0,
        unique_landing_pages: sessionData.landing_pages?.length || 0,
        unique_sources: sessionData.sources?.length || 0,
        pageviews_returned: pageviews.length
      },
      metadata: {
        ip_addresses: sessionData.ip_addresses || [],
        landing_pages: sessionData.landing_pages || [],
        sources: sessionData.sources || []
      },
      pageviews: includeRawPageviews ? pageviews : pageviews.map(pv => ({
        timestamp: pv.timestamp,
        landing_page: pv.landing_page,
        source: pv.source,
        utm_campaign: pv.utm_campaign,
        ip_address: pv.ip_address,
        screen_resolution: pv.screen_resolution
      }))
    };
    
  } catch (error) {
    console.error('❌ Session attribution index query error:', error);
    return {
      session_id: sessionId,
      error: 'Failed to query session attribution index',
      message: error.message
    };
  }
}

// Query landing page attribution index
async function queryLandingPageAttributionIndex(redis, landingPage, limit, includeRawPageviews) {
  try {
    const encodedLP = encodeLandingPageForKey(landingPage);
    const lpKey = `attribution_index_v1_landing:${encodedLP}`;
    console.log(`🔍 Querying landing page attribution index: ${lpKey}`);
    
    const result = await redis(`get/${lpKey}`, 3000);
    
    if (!result?.result) {
      return {
        landing_page: landingPage,
        found: false,
        message: 'No pageviews found for this landing page in attribution indexes'
      };
    }
    
    const lpData = JSON.parse(decodeURIComponent(result.result));
    
    let pageviews = lpData.pageviews || [];
    if (limit && pageviews.length > limit) {
      pageviews = pageviews.slice(0, limit);
    }
    
    return {
      landing_page: landingPage,
      found: true,
      summary: {
        pageview_count: lpData.pageview_count,
        latest_timestamp: lpData.latest_timestamp,
        unique_ips: lpData.ip_addresses?.length || 0,
        unique_sessions: lpData.session_ids?.length || 0,
        unique_sources: lpData.sources?.length || 0,
        pageviews_returned: pageviews.length
      },
      metadata: {
        ip_addresses: lpData.ip_addresses || [],
        session_ids: lpData.session_ids || [],
        sources: lpData.sources || []
      },
      pageviews: includeRawPageviews ? pageviews : pageviews.map(pv => ({
        timestamp: pv.timestamp,
        source: pv.source,
        utm_campaign: pv.utm_campaign,
        ip_address: pv.ip_address,
        session_id: pv.session_id
      }))
    };
    
  } catch (error) {
    console.error('❌ Landing page attribution index query error:', error);
    return {
      landing_page: landingPage,
      error: 'Failed to query landing page attribution index',
      message: error.message
    };
  }
}

// Query source attribution index
async function querySourceAttributionIndex(redis, source, limit, includeRawPageviews) {
  try {
    const encodedSource = encodeSourceForKey(source);
    const sourceKey = `attribution_index_v1_source:${encodedSource}`;
    console.log(`🔍 Querying source attribution index: ${sourceKey}`);
    
    const result = await redis(`get/${sourceKey}`, 3000);
    
    if (!result?.result) {
      return {
        source: source,
        found: false,
        message: 'No pageviews found for this source in attribution indexes'
      };
    }
    
    const sourceData = JSON.parse(decodeURIComponent(result.result));
    
    let pageviews = sourceData.pageviews || [];
    if (limit && pageviews.length > limit) {
      pageviews = pageviews.slice(0, limit);
    }
    
    return {
      source: source,
      found: true,
      summary: {
        pageview_count: sourceData.pageview_count,
        latest_timestamp: sourceData.latest_timestamp,
        unique_ips: sourceData.ip_addresses?.length || 0,
        unique_sessions: sourceData.session_ids?.length || 0,
        unique_landing_pages: sourceData.landing_pages?.length || 0,
        pageviews_returned: pageviews.length
      },
      metadata: {
        ip_addresses: sourceData.ip_addresses || [],
        session_ids: sourceData.session_ids || [],
        landing_pages: sourceData.landing_pages || []
      },
      pageviews: includeRawPageviews ? pageviews : pageviews.map(pv => ({
        timestamp: pv.timestamp,
        landing_page: pv.landing_page,
        utm_campaign: pv.utm_campaign,
        ip_address: pv.ip_address,
        session_id: pv.session_id
      }))
    };
    
  } catch (error) {
    console.error('❌ Source attribution index query error:', error);
    return {
      source: source,
      error: 'Failed to query source attribution index',
      message: error.message
    };
  }
}

// Utility functions
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

function encodeLandingPageForKey(landingPage) {
  return encodeURIComponent(landingPage).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 100);
}

function encodeSourceForKey(source) {
  return encodeURIComponent(source).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
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
