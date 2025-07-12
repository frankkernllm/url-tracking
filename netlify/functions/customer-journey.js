// customer-journey.js - FIXED with bulletproof CORS and timeout handling
// Shows complete customer journey from first visit to conversion

// ✅ BULLETPROOF CORS HEADERS - Apply to ALL functions
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Max-Age': '86400',
  'Content-Type': 'application/json'
};

// ✅ CORS-SAFE RESPONSE HELPER - Ensures CORS headers are ALWAYS sent
function createCorsResponse(statusCode, body, additionalHeaders = {}) {
  return {
    statusCode,
    headers: { ...CORS_HEADERS, ...additionalHeaders },
    body: typeof body === 'string' ? body : JSON.stringify(body)
  };
}

// ✅ TIMEOUT WRAPPER - Prevents 504 errors
function withTimeout(promise, timeoutMs = 25000) {
  return Promise.race([
    promise,
    new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Function timeout')), timeoutMs)
    )
  ]);
}

exports.handler = async (event, context) => {
  // ✅ IMMEDIATE OPTIONS HANDLING - Critical for CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    console.log('🔧 CORS preflight request received');
    return createCorsResponse(200, { message: 'CORS preflight successful' });
  }

  // ✅ WRAP ENTIRE FUNCTION IN TIMEOUT AND CORS ERROR HANDLING
  try {
    return await withTimeout(processCustomerJourney(event, context), 25000);
  } catch (error) {
    console.error('❌ Customer journey error:', error);
    
    // ✅ ENSURE CORS HEADERS EVEN IN ERROR CASES
    return createCorsResponse(500, {
      success: false,
      error: 'Customer journey processing failed',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
};

// Main processing function with optimized performance
async function processCustomerJourney(event, context) {
  const startTime = Date.now();
  
  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return createCorsResponse(401, { error: 'Invalid API key' });
  }

  // Redis helper with timeout
  const redis = (path, timeoutMs = 3000) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return withTimeout(
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()),
      timeoutMs
    );
  };

  try {
    const {
      email,
      conversion_timestamp,
      ips_to_check = [],
      session_id,
      device_signature,
      screen_value,
      gpu_signature,
      journey_window_days = 7
    } = JSON.parse(event.body || '{}');

    console.log(`🛣️ CUSTOMER JOURNEY: Mapping journey for ${email}`);
    console.log(`   🕐 Window: ${journey_window_days} days before ${conversion_timestamp}`);

    const conversionTime = new Date(conversion_timestamp).getTime();
    const windowStart = conversionTime - (journey_window_days * 24 * 60 * 60 * 1000);
    
    // ✅ OPTIMIZED: Quick pageview search with strict time limits
    const allPageviews = await findCustomerPageviewsOptimized(redis, {
      ips_to_check,
      session_id,
      device_signature,
      screen_value,
      gpu_signature,
      windowStart,
      conversionTime
    });

    // Build journey
    const journey = buildCustomerJourney(allPageviews, {
      email,
      conversion_timestamp,
      conversionTime
    });

    const processingTime = Date.now() - startTime;
    console.log(`🏁 Journey mapped: ${journey.pageviews.length} pageviews in ${processingTime}ms`);

    return createCorsResponse(200, {
      customer: {
        email,
        conversion_timestamp
      },
      journey: journey,
      summary: {
        total_pageviews: journey.pageviews.length,
        unique_pages: journey.unique_pages.length,
        total_duration_minutes: journey.total_duration_minutes,
        attribution_score: journey.attribution_score,
        first_touch: journey.first_touch,
        last_touch: journey.last_touch
      },
      processing_time_ms: processingTime
    });

  } catch (error) {
    console.error('❌ Customer journey processing error:', error);
    return createCorsResponse(500, {
      error: 'Journey processing failed',
      message: error.message
    });
  }
}

// ✅ OPTIMIZED: Fast pageview search with time limits
async function findCustomerPageviewsOptimized(redis, searchParams) {
  const { ips_to_check, session_id, device_signature, windowStart, conversionTime } = searchParams;
  
  let allPageviews = [];
  const foundKeys = new Set();
  const maxSearchTime = 10000; // 10 second limit
  const searchStartTime = Date.now();

  console.log('🔍 Optimized pageview search for customer journey...');

  try {
    // PRIORITY 1: Session ID (fastest, most reliable)
    if (session_id && Date.now() - searchStartTime < maxSearchTime) {
      console.log(`   🎯 Session search: ${session_id}`);
      const sessionPageviews = await findPageviewsBySessionOptimized(redis, session_id, windowStart, conversionTime);
      sessionPageviews.forEach(pv => {
        if (!foundKeys.has(pv._redis_key)) {
          pv.match_method = 'session_id';
          pv.confidence = 300;
          allPageviews.push(pv);
          foundKeys.add(pv._redis_key);
        }
      });
      console.log(`   ✅ Session: ${sessionPageviews.length} pageviews`);
    }

    // PRIORITY 2: Device Signature (if session didn't find much)
    if (device_signature && allPageviews.length < 5 && Date.now() - searchStartTime < maxSearchTime) {
      console.log(`   🔐 Device search: ${device_signature.substring(0, 10)}...`);
      const devicePageviews = await findPageviewsByDeviceOptimized(redis, device_signature, windowStart, conversionTime);
      devicePageviews.forEach(pv => {
        if (!foundKeys.has(pv._redis_key)) {
          pv.match_method = 'device_signature';
          pv.confidence = 220;
          allPageviews.push(pv);
          foundKeys.add(pv._redis_key);
        }
      });
      console.log(`   ✅ Device: ${devicePageviews.length} additional pageviews`);
    }

    // PRIORITY 3: IP Addresses (limited search)
    if (ips_to_check.length > 0 && allPageviews.length < 10 && Date.now() - searchStartTime < maxSearchTime) {
      console.log(`   📍 IP search: ${ips_to_check.slice(0, 2).join(', ')} (limited)`);
      
      // Only check first 2 IPs to save time
      for (let i = 0; i < Math.min(ips_to_check.length, 2); i++) {
        if (Date.now() - searchStartTime >= maxSearchTime) break;
        
        const ip = ips_to_check[i];
        if (!ip || ip === 'unknown') continue;
        
        const ipPageviews = await findPageviewsByIpOptimized(redis, ip, windowStart, conversionTime);
        ipPageviews.forEach(pv => {
          if (!foundKeys.has(pv._redis_key)) {
            pv.match_method = i === 0 ? 'primary_ip' : 'conversion_ip';
            pv.confidence = i === 0 ? 280 : 260;
            allPageviews.push(pv);
            foundKeys.add(pv._redis_key);
          }
        });
        console.log(`   ✅ IP ${ip}: ${ipPageviews.length} additional pageviews`);
      }
    }

    const searchTime = Date.now() - searchStartTime;
    console.log(`🎯 Search complete: ${allPageviews.length} pageviews in ${searchTime}ms`);
    
    return allPageviews;

  } catch (error) {
    console.error('❌ Optimized pageview search failed:', error);
    return allPageviews; // Return what we found so far
  }
}

// ✅ OPTIMIZED: Fast session search using direct lookup
async function findPageviewsBySessionOptimized(redis, sessionId, windowStart, conversionTime) {
  try {
    // Try direct session index lookup first
    const sessionKey = `attribution_session_${sessionId}`;
    const sessionResult = await redis(`get/${sessionKey}`, 2000);
    
    if (sessionResult.result) {
      const attributionResult = await redis(`get/${sessionResult.result}`, 2000);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          pageview._redis_key = sessionResult.result;
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ Optimized session search failed:', error);
    return [];
  }
}

// ✅ OPTIMIZED: Fast device search using direct lookup
async function findPageviewsByDeviceOptimized(redis, deviceSig, windowStart, conversionTime) {
  try {
    const deviceKey = `attribution_fp_${deviceSig.slice(-20)}`;
    const deviceResult = await redis(`get/${deviceKey}`, 2000);
    
    if (deviceResult.result) {
      const attributionResult = await redis(`get/${deviceResult.result}`, 2000);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          pageview._redis_key = deviceResult.result;
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ Optimized device search failed:', error);
    return [];
  }
}

// ✅ OPTIMIZED: Fast IP search using direct lookup
async function findPageviewsByIpOptimized(redis, ip, windowStart, conversionTime) {
  try {
    const encodedIp = ip.replace(/:/g, '_');
    const ipKey = `attribution_ip_${encodedIp}`;
    const ipResult = await redis(`get/${ipKey}`, 2000);
    
    if (ipResult.result) {
      const attributionResult = await redis(`get/${ipResult.result}`, 2000);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          pageview._redis_key = ipResult.result;
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    console.warn('⚠️ Optimized IP search failed:', error);
    return [];
  }
}

// ✅ SIMPLIFIED: Build customer journey (essential fields only)
function buildCustomerJourney(pageviews, conversionInfo) {
  if (pageviews.length === 0) {
    return {
      pageviews: [],
      unique_pages: [],
      total_duration_minutes: 0,
      attribution_score: 0,
      first_touch: null,
      last_touch: null
    };
  }

  // Sort chronologically
  const sortedPageviews = pageviews.sort((a, b) => 
    new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  // Calculate basic metrics
  const firstTouch = sortedPageviews[0];
  const lastTouch = sortedPageviews[sortedPageviews.length - 1];
  const uniquePages = [...new Set(sortedPageviews.map(pv => pv.landing_page))];
  
  const journeyStartTime = new Date(firstTouch.timestamp).getTime();
  const journeyEndTime = new Date(lastTouch.timestamp).getTime();
  const totalDurationMinutes = Math.round((journeyEndTime - journeyStartTime) / (1000 * 60));
  
  const attributionScore = Math.max(...sortedPageviews.map(pv => pv.confidence || 0));

  return {
    pageviews: sortedPageviews.map((pv, index) => ({
      timestamp: pv.timestamp,
      landing_page: pv.landing_page,
      source: pv.source,
      utm_source: pv.utm_source,
      utm_campaign: pv.utm_campaign,
      utm_medium: pv.utm_medium,
      session_id: pv.session_id,
      attribution_method: pv.match_method,
      confidence: pv.confidence,
      journey_position: index + 1
    })),
    unique_pages: uniquePages,
    total_duration_minutes: totalDurationMinutes,
    attribution_score: attributionScore,
    first_touch: {
      timestamp: firstTouch.timestamp,
      landing_page: firstTouch.landing_page,
      source: firstTouch.source || firstTouch.utm_source,
      attribution_method: firstTouch.match_method
    },
    last_touch: {
      timestamp: lastTouch.timestamp,
      landing_page: lastTouch.landing_page,
      source: lastTouch.source || lastTouch.utm_source,
      attribution_method: lastTouch.match_method
    }
  };
}
