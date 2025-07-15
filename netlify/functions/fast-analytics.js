// fast-analytics.js - Lightning Fast Dashboard Data Using Pre-Built Indexes
// Path: netlify/functions/fast-analytics.js  
// Purpose: Ultra-fast analytics using pre-extracted & indexed data

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('⚡ FAST-ANALYTICS: Starting lightning-fast analytics query');
    const startTime = Date.now();
    
    // Parse query parameters
    const queryParams = event.queryStringParameters || {};
    const startDate = queryParams.start_date || getDateDaysAgo(7);
    const endDate = queryParams.end_date || getDateDaysAgo(0);
    const limit = parseInt(queryParams.limit || '1000');
    
    console.log(`📅 Date range: ${startDate} to ${endDate} (limit: ${limit})`);
    
    const redis = initializeRedis();
    
    // STEP 1: Get pageviews from pre-built indexes (FAST)
    console.log('📄 Fetching pageviews from indexes...');
    const pageviews = await getPageviewsFromIndexes(redis, startDate, endDate, limit);
    console.log(`✅ Found ${pageviews.length} pageviews`);
    
    // STEP 2: Get conversions from pre-built indexes (FAST)  
    console.log('💰 Fetching conversions from indexes...');
    const conversions = await getConversionsFromIndexes(redis, startDate, endDate, limit);
    console.log(`✅ Found ${conversions.length} conversions`);
    
    // STEP 3: Quick cross-reference for attribution analysis (FAST)
    console.log('🔗 Cross-referencing attribution data...');
    const attributionAnalysis = performFastAttribution(pageviews, conversions);
    
    // STEP 4: Generate summary statistics (FAST)
    const analytics = generateAnalyticsSummary(pageviews, conversions, attributionAnalysis);
    
    const totalTime = Date.now() - startTime;
    console.log(`⚡ Fast analytics complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        // Dashboard expects arrays directly
        page_views: pageviews,
        conversions: conversions,
        
        // Summary statistics
        total_page_views: pageviews.length,
        total_conversions: conversions.length,
        unique_visitors: analytics.unique_visitors,
        total_revenue: analytics.total_revenue.toFixed(2),
        conversion_rate: analytics.conversion_rate,
        
        // Attribution analysis
        attribution_summary: attributionAnalysis.summary,
        attribution_breakdown: attributionAnalysis.breakdown,
        
        // Date range info
        date_range: {
          start: startDate,
          end: endDate,
          days: getDaysBetween(startDate, endDate),
          calculation_method: 'fast_indexed_lookup'
        },
        
        // Performance metrics
        processing_stats: {
          execution_time_ms: totalTime,
          data_source: 'pre_built_indexes',
          pageview_index_hits: analytics.pageview_index_hits,
          conversion_index_hits: analytics.conversion_index_hits,
          cross_reference_time_ms: attributionAnalysis.processing_time_ms,
          filtered_null_ips: analytics.filtered_null_ips // NEW: Track filtered conversions
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Fast analytics error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Fast analytics failed', 
        message: error.message 
      })
    };
  }
};

// Get pageviews from pre-built indexes (instead of scanning all data)
async function getPageviewsFromIndexes(redis, startDate, endDate, limit) {
  console.log('📄 Loading pageviews from IP indexes...');
  
  const pageviews = [];
  let indexHits = 0;
  
  try {
    // Scan for pageview IP indexes
    let cursor = '0';
    do {
      const scanResult = await redis(`scan/${cursor}/match/pageview_index_ip:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const indexKeys = scanResult.result[1] || [];
      
      // Load pageviews from each IP index
      for (const indexKey of indexKeys) {
        if (pageviews.length >= limit) break;
        
        try {
          const indexData = await redis(`get/${indexKey}`);
          if (indexData?.result) {
            const ipIndex = JSON.parse(decodeURIComponent(indexData.result));
            indexHits++;
            
            // Add pageviews from this IP that are within date range
            if (ipIndex.pageviews && Array.isArray(ipIndex.pageviews)) {
              for (const pv of ipIndex.pageviews) {
                if (isWithinDateRange(pv.timestamp, startDate, endDate)) {
                  pageviews.push({
                    timestamp: pv.timestamp,
                    landing_page: pv.landing_page,
                    source: pv.source || 'direct',
                    utm_source: pv.utm_source,
                    utm_campaign: pv.utm_campaign,
                    utm_medium: pv.utm_medium,
                    ip_address: ipIndex.ip_address,
                    session_id: pv.session_id,
                    device_signature: pv.device_signature,
                    referrer_url: pv.referrer_url
                  });
                }
                
                if (pageviews.length >= limit) break;
              }
            }
          }
        } catch (indexError) {
          console.warn(`⚠️ Failed to load pageview index ${indexKey}:`, indexError.message);
        }
      }
      
    } while (cursor !== '0' && pageviews.length < limit);
    
    // Sort by timestamp
    pageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    console.log(`✅ Loaded ${pageviews.length} pageviews from ${indexHits} IP indexes`);
    return pageviews;
    
  } catch (error) {
    console.error('❌ Pageview index loading failed:', error);
    return [];
  }
}

// Get conversions from pre-built indexes (FAST!) - WITH NULL IP FILTERING
async function getConversionsFromIndexes(redis, startDate, endDate, limit) {
  console.log('💰 Loading conversions from date indexes...');
  
  const conversions = [];
  let indexHits = 0;
  let filteredOutCount = 0; // Track how many we filter out
  
  try {
    // Generate date keys for the range
    const dateKeys = generateDateKeys(startDate, endDate);
    
    for (const dateKey of dateKeys) {
      if (conversions.length >= limit) break;
      
      try {
        const indexKey = `conversion_index_date:${dateKey}`;
        const indexData = await redis(`get/${indexKey}`);
        
        if (indexData?.result) {
          const dateIndex = JSON.parse(decodeURIComponent(indexData.result));
          indexHits++;
          
          // Add conversions from this date WITH NULL IP FILTERING
          if (dateIndex.conversions && Array.isArray(dateIndex.conversions)) {
            for (const conversion of dateIndex.conversions) {
              if (conversions.length >= limit) break;
              
              // 🚫 FILTER OUT: Only filter obviously bogus conversions, not legitimate unattributed ones
              // Keep conversions that track.js legitimately stored, even with null IPs
              if ((!conversion.order_id || conversion.order_id === 'unknown') && 
                  (!conversion.email || conversion.email === 'unknown') &&
                  (!conversion.ip_address || conversion.ip_address === null || conversion.ip_address === 'null' || conversion.ip_address === '')) {
                
                filteredOutCount++;
                console.log(`🚫 Filtered out bogus conversion: order_id=${conversion.order_id}, email=${conversion.email}, ip=${conversion.ip_address}`);
                continue; // Skip this conversion
              }
              
              conversions.push({
                timestamp: conversion.timestamp,
                email: conversion.email,
                order_total: conversion.order_total,
                order_id: conversion.order_id,
                attribution_found: conversion.attribution_found,
                attribution_method: conversion.attribution_method,
                source: conversion.source,
                campaign: conversion.campaign,
                medium: conversion.medium,
                landing_page: conversion.landing_page,
                ip_address: conversion.ip_address, // This is guaranteed to be valid now
                session_id: conversion.session_id,
                event_type: conversion.event_type
              });
            }
          }
        }
      } catch (indexError) {
        console.warn(`⚠️ Failed to load conversion index for ${dateKey}:`, indexError.message);
      }
    }
    
    // Sort by timestamp
    conversions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    console.log(`✅ Loaded ${conversions.length} valid conversions from ${indexHits} date indexes`);
    console.log(`🚫 Filtered out ${filteredOutCount} conversions with null/invalid IP addresses`);
    
    // Store filtered count for reporting
    conversions._filteredCount = filteredOutCount;
    
    return conversions;
    
  } catch (error) {
    console.error('❌ Conversion index loading failed:', error);
    return [];
  }
}

// Fast attribution cross-reference
function performFastAttribution(pageviews, conversions) {
  const analysisStart = Date.now();
  console.log('🔗 Performing fast attribution cross-reference...');
  
  const attribution = {
    attributed: 0,
    unattributed: 0,
    methods: {},
    sources: {},
    summary: {},
    breakdown: {}
  };
  
  // Quick analysis without heavy computation
  for (const conversion of conversions) {
    if (conversion.attribution_found) {
      attribution.attributed++;
      
      // Count attribution methods
      const method = conversion.attribution_method || 'unknown';
      attribution.methods[method] = (attribution.methods[method] || 0) + 1;
      
      // Count sources
      const source = conversion.source || 'unknown';
      attribution.sources[source] = (attribution.sources[source] || 0) + 1;
    } else {
      attribution.unattributed++;
    }
  }
  
  // Calculate summary
  const totalConversions = conversions.length;
  attribution.summary = {
    total_conversions: totalConversions,
    attributed_conversions: attribution.attributed,
    unattributed_conversions: attribution.unattributed,
    attribution_rate: totalConversions > 0 ? 
      ((attribution.attributed / totalConversions) * 100).toFixed(1) : '0.0'
  };
  
  // Top sources and methods
  attribution.breakdown = {
    top_sources: Object.entries(attribution.sources)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 10)
      .map(([source, count]) => ({ source, count })),
    
    attribution_methods: Object.entries(attribution.methods)
      .sort(([,a], [,b]) => b - a)
      .map(([method, count]) => ({ method, count }))
  };
  
  const processingTime = Date.now() - analysisStart;
  console.log(`✅ Attribution analysis complete in ${processingTime}ms`);
  
  return {
    ...attribution,
    processing_time_ms: processingTime
  };
}

// Generate analytics summary
function generateAnalyticsSummary(pageviews, conversions, attributionAnalysis) {
  console.log('📊 Generating analytics summary...');
  
  // Unique visitors (by IP)
  const uniqueIPs = new Set();
  pageviews.forEach(pv => {
    if (pv.ip_address && pv.ip_address !== 'unknown') {
      uniqueIPs.add(pv.ip_address);
    }
  });
  
  // Total revenue
  const totalRevenue = conversions.reduce((sum, conv) => 
    sum + (parseFloat(conv.order_total) || 0), 0
  );
  
  // Conversion rate
  const conversionRate = uniqueIPs.size > 0 ? 
    ((conversions.length / uniqueIPs.size) * 100).toFixed(2) : '0.00';
  
  return {
    unique_visitors: uniqueIPs.size,
    total_revenue: totalRevenue,
    conversion_rate: conversionRate,
    pageview_index_hits: pageviews.length, // Approximate
    conversion_index_hits: conversions.length, // Approximate
    filtered_null_ips: conversions._filteredCount || 0 // NEW: Include filtered count
  };
}

// Helper functions
function getDateDaysAgo(daysAgo) {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return date.toISOString().split('T')[0];
}

function getDaysBetween(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  return Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
}

function isWithinDateRange(timestamp, startDate, endDate) {
  const date = new Date(timestamp).toISOString().split('T')[0];
  return date >= startDate && date <= endDate;
}

function generateDateKeys(startDate, endDate) {
  const keys = [];
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    keys.push(key);
  }
  
  return keys;
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 2000) => {
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
