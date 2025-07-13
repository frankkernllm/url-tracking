// netlify/functions/query-customer-journeys.js
// OPTIMIZED Customer Journey Query Engine - Efficient Redis Scanning
// Uses same proven pattern as build-customer-journeys.js (no bulk loading)

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Invalid API key' })
    };
  }

  try {
    console.log('🔍 OPTIMIZED JOURNEY QUERY: Starting efficient scanning...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      date_range_days = 7,
      limit = 100,
      offset = 0,
      sample_only = false,
      cleanup_duplicates = false,
      analytics_mode = false,
      journey_id = null,
      customer_email = null,
      order_id = null
    } = body;
    
    console.log(`📊 Query Parameters: ${date_range_days} days, limit: ${limit}, sample: ${sample_only}, cleanup: ${cleanup_duplicates}`);
    
    // Handle different query modes
    if (cleanup_duplicates) {
      return await handleDuplicateCleanup(redis, maxProcessingTime - (Date.now() - startTime));
    }
    
    if (sample_only) {
      return await handleSampleQuery(redis, limit, maxProcessingTime - (Date.now() - startTime));
    }
    
    if (analytics_mode) {
      return await handleAnalyticsQuery(redis, date_range_days, maxProcessingTime - (Date.now() - startTime));
    }
    
    // Default: Efficient journey search
    const searchResults = await performEfficientJourneySearch(redis, {
      date_range_days,
      limit,
      offset,
      journey_id,
      customer_email,
      order_id
    }, maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Optimized query complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_optimized: true,
        query_results: searchResults.journeys,
        query_summary: {
          journeys_found: searchResults.journeys_found,
          journeys_returned: searchResults.journeys.length,
          keys_scanned: searchResults.keys_scanned,
          processing_time_ms: totalTime,
          has_more_results: searchResults.has_more_results,
          next_offset: offset + searchResults.journeys.length
        },
        performance: {
          journeys_per_second: Math.round(searchResults.journeys_found / (totalTime / 1000)),
          scanning_efficiency: 'redis_cursor_based',
          memory_efficient: true
        },
        pagination: {
          current_limit: limit,
          current_offset: offset,
          suggested_next_call: searchResults.has_more_results ? {
            limit: limit,
            offset: offset + searchResults.journeys.length
          } : null
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Optimized journey query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Optimized journey query failed', 
        message: error.message 
      })
    };
  }
};

// OPTIMIZED: Efficient journey search using Redis scanning (no bulk loading)
async function performEfficientJourneySearch(redis, searchParams, maxTime) {
  const searchStartTime = Date.now();
  const { date_range_days, limit, offset, journey_id, customer_email, order_id } = searchParams;
  
  console.log(`🔍 Efficient journey search: ${date_range_days} days, limit: ${limit}, offset: ${offset}`);
  
  // Calculate date filter
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - date_range_days);
  const cutoffTimestamp = cutoffDate.getTime();
  
  const journeys = [];
  let keysScanned = 0;
  let journeysFound = 0;
  let journeysSkipped = 0; // For offset handling
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  try {
    do {
      // Check timeout
      if (Date.now() - searchStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during journey search, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Process keys in small batches
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - searchStartTime > maxTime - 2000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Apply filters during scanning (not after loading)
              if (!passesFilters(journey, cutoffTimestamp, journey_id, customer_email, order_id)) {
                return null;
              }
              
              journeysFound++;
              
              // Handle offset (skip journeys before our starting point)
              if (journeysSkipped < offset) {
                journeysSkipped++;
                return null;
              }
              
              // Stop if we have enough results
              if (journeys.length >= limit) {
                return null;
              }
              
              return {
                journey_id: journey.journey_id,
                customer_email: journey.customer_email,
                conversion_timestamp: journey.conversion_timestamp,
                conversion_order_id: journey.conversion_order_id,
                conversion_value: journey.conversion_value,
                journey_span_hours: journey.journey_span_hours,
                total_touchpoints: journey.total_touchpoints,
                unique_sources: journey.unique_sources,
                first_click_source: journey.first_click_source,
                last_click_source: journey.last_click_source,
                cross_session_journey: journey.cross_session_journey,
                cross_device_journey: journey.cross_device_journey,
                attribution_confidence_avg: journey.attribution_confidence_avg,
                touchpoints: journey.touchpoints || [],
                created_at: journey.created_at,
                _redis_key: key
              };
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        journeys.push(...validResults);
        
        // Stop if we have enough results
        if (journeys.length >= limit) {
          break;
        }
      }
      
      // Stop if we have enough results
      if (journeys.length >= limit) {
        break;
      }
      
      if (iterations % 5 === 0) {
        console.log(`🔍 Search progress: ${keysScanned} keys scanned, ${journeysFound} journeys found, ${journeys.length} returned`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations && journeys.length < limit);
    
    const hasMoreResults = cursor !== '0' || journeysFound > (offset + journeys.length);
    
    console.log(`✅ Efficient search complete: ${journeys.length} journeys returned from ${journeysFound} found`);
    
    return {
      journeys: journeys,
      journeys_found: journeysFound,
      keys_scanned: keysScanned,
      has_more_results: hasMoreResults,
      processing_time_ms: Date.now() - searchStartTime
    };
    
  } catch (error) {
    console.error('❌ Efficient journey search error:', error);
    return {
      journeys: journeys,
      journeys_found: journeysFound,
      keys_scanned: keysScanned,
      has_more_results: false,
      error: error.message
    };
  }
}

// Filter function (applied during scanning for efficiency)
function passesFilters(journey, cutoffTimestamp, journey_id, customer_email, order_id) {
  // Date filter
  if (journey.conversion_timestamp) {
    const journeyTime = new Date(journey.conversion_timestamp).getTime();
    if (journeyTime < cutoffTimestamp) {
      return false;
    }
  }
  
  // Specific filters
  if (journey_id && journey.journey_id !== journey_id) return false;
  if (customer_email && journey.customer_email !== customer_email) return false;
  if (order_id && journey.conversion_order_id !== order_id) return false;
  
  return true;
}

// OPTIMIZED: Sample query (gets first N journeys quickly)
async function handleSampleQuery(redis, limit, maxTime) {
  console.log(`📊 Sample query: getting first ${limit} journeys quickly...`);
  
  const sampleStartTime = Date.now();
  const samples = [];
  let cursor = '0';
  let keysScanned = 0;
  
  try {
    do {
      if (Date.now() - sampleStartTime > maxTime - 2000) break;
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/50`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      
      // Process just enough to get samples
      for (const key of keys.slice(0, limit - samples.length)) {
        try {
          const journeyData = await redis(`get/${key}`, 500);
          if (journeyData?.result) {
            const journey = JSON.parse(decodeURIComponent(journeyData.result));
            samples.push({
              journey_id: journey.journey_id,
              customer_email: journey.customer_email,
              conversion_order_id: journey.conversion_order_id,
              conversion_timestamp: journey.conversion_timestamp,
              total_touchpoints: journey.total_touchpoints,
              first_click_source: journey.first_click_source,
              last_click_source: journey.last_click_source,
              journey_span_hours: journey.journey_span_hours,
              created_at: journey.created_at,
              _redis_key: key
            });
          }
        } catch (parseError) {
          // Skip invalid data
        }
        
        if (samples.length >= limit) break;
      }
      
      if (samples.length >= limit) break;
      
    } while (cursor !== '0' && samples.length < limit);
    
    const processingTime = Date.now() - sampleStartTime;
    console.log(`✅ Sample query complete: ${samples.length} samples in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        sample_query: true,
        samples: samples,
        sample_summary: {
          samples_returned: samples.length,
          keys_scanned: keysScanned,
          processing_time_ms: processingTime
        }
      })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Sample query failed', message: error.message })
    };
  }
}

// OPTIMIZED: Analytics query (efficient aggregation)
async function handleAnalyticsQuery(redis, dateRangeDays, maxTime) {
  console.log(`📈 Analytics query: aggregating journey data for ${dateRangeDays} days...`);
  
  const analyticsStartTime = Date.now();
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - dateRangeDays);
  const cutoffTimestamp = cutoffDate.getTime();
  
  const analytics = {
    total_journeys: 0,
    multi_touchpoint_journeys: 0,
    cross_session_journeys: 0,
    cross_device_journeys: 0,
    total_conversion_value: 0,
    unique_customers: new Set(),
    sources: {},
    attribution_methods: {},
    journey_span_distribution: { under_1h: 0, '1-24h': 0, '1-7d': 0, over_7d: 0 },
    touchpoint_distribution: { single: 0, '2-3': 0, '4-10': 0, over_10: 0 }
  };
  
  let cursor = '0';
  let keysScanned = 0;
  let iterations = 0;
  const maxIterations = 15;
  
  try {
    do {
      if (Date.now() - analyticsStartTime > maxTime - 3000) break;
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Process keys in batches for analytics
      const batchSize = 25;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - analyticsStartTime > maxTime - 2000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 800);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Date filter
              if (journey.conversion_timestamp) {
                const journeyTime = new Date(journey.conversion_timestamp).getTime();
                if (journeyTime >= cutoffTimestamp) {
                  return journey;
                }
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(journey => journey !== null);
        
        // Aggregate analytics data
        for (const journey of validJourneys) {
          analytics.total_journeys++;
          
          if (journey.total_touchpoints > 1) analytics.multi_touchpoint_journeys++;
          if (journey.cross_session_journey) analytics.cross_session_journeys++;
          if (journey.cross_device_journey) analytics.cross_device_journeys++;
          
          analytics.total_conversion_value += parseFloat(journey.conversion_value || 0);
          analytics.unique_customers.add(journey.customer_email);
          
          // Source aggregation
          const firstSource = journey.first_click_source || 'unknown';
          analytics.sources[firstSource] = (analytics.sources[firstSource] || 0) + 1;
          
          // Journey span distribution
          const spanHours = journey.journey_span_hours || 0;
          if (spanHours < 1) analytics.journey_span_distribution.under_1h++;
          else if (spanHours < 24) analytics.journey_span_distribution['1-24h']++;
          else if (spanHours < 168) analytics.journey_span_distribution['1-7d']++;
          else analytics.journey_span_distribution.over_7d++;
          
          // Touchpoint distribution
          const touchpoints = journey.total_touchpoints || 1;
          if (touchpoints === 1) analytics.touchpoint_distribution.single++;
          else if (touchpoints <= 3) analytics.touchpoint_distribution['2-3']++;
          else if (touchpoints <= 10) analytics.touchpoint_distribution['4-10']++;
          else analytics.touchpoint_distribution.over_10++;
        }
      }
      
      if (iterations % 3 === 0) {
        console.log(`📈 Analytics progress: ${analytics.total_journeys} journeys processed`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
    // Finalize analytics
    const uniqueCustomerCount = analytics.unique_customers.size;
    delete analytics.unique_customers; // Remove Set for JSON serialization
    
    const processingTime = Date.now() - analyticsStartTime;
    console.log(`✅ Analytics complete: ${analytics.total_journeys} journeys in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        analytics_query: true,
        date_range_days: dateRangeDays,
        analytics: {
          ...analytics,
          unique_customers: uniqueCustomerCount,
          avg_conversion_value: analytics.total_journeys > 0 ? 
            (analytics.total_conversion_value / analytics.total_journeys).toFixed(2) : 0,
          multi_touchpoint_rate: analytics.total_journeys > 0 ? 
            ((analytics.multi_touchpoint_journeys / analytics.total_journeys) * 100).toFixed(1) : 0,
          cross_session_rate: analytics.total_journeys > 0 ? 
            ((analytics.cross_session_journeys / analytics.total_journeys) * 100).toFixed(1) : 0
        },
        analytics_summary: {
          journeys_analyzed: analytics.total_journeys,
          keys_scanned: keysScanned,
          processing_time_ms: processingTime
        }
      })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Analytics query failed', message: error.message })
    };
  }
}

// CLEANUP: Duplicate detection and removal
async function handleDuplicateCleanup(redis, maxTime) {
  console.log('🧹 Duplicate cleanup: detecting and removing duplicate journey records...');
  
  const cleanupStartTime = Date.now();
  const journeyMap = new Map(); // order_id -> [journey_keys]
  let cursor = '0';
  let keysScanned = 0;
  let duplicatesFound = 0;
  let duplicatesRemoved = 0;
  
  try {
    // Step 1: Scan all journeys and group by order_id
    do {
      if (Date.now() - cleanupStartTime > maxTime - 10000) break;
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      
      // Group keys by order_id
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - cleanupStartTime > maxTime - 8000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 500);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              if (journey.conversion_order_id) {
                const orderId = journey.conversion_order_id;
                if (!journeyMap.has(orderId)) {
                  journeyMap.set(orderId, []);
                }
                journeyMap.get(orderId).push({
                  key: key,
                  created_at: journey.created_at,
                  journey_id: journey.journey_id
                });
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
        });
        
        await Promise.all(batchPromises);
      }
      
    } while (cursor !== '0');
    
    // Step 2: Find and remove duplicates
    console.log(`🔍 Analyzing ${journeyMap.size} unique order IDs for duplicates...`);
    
    const duplicateOrderIds = [];
    for (const [orderId, journeys] of journeyMap.entries()) {
      if (journeys.length > 1) {
        duplicatesFound += journeys.length - 1;
        duplicateOrderIds.push({
          order_id: orderId,
          journey_count: journeys.length,
          journeys: journeys
        });
      }
    }
    
    console.log(`🚨 Found ${duplicatesFound} duplicate journeys across ${duplicateOrderIds.length} order IDs`);
    
    // Step 3: Remove duplicates (keep the most recent)
    for (const duplicate of duplicateOrderIds) {
      if (Date.now() - cleanupStartTime > maxTime - 2000) break;
      
      // Sort by created_at (keep most recent)
      const sortedJourneys = duplicate.journeys.sort((a, b) => 
        new Date(b.created_at) - new Date(a.created_at)
      );
      
      // Remove all but the most recent
      const journeysToRemove = sortedJourneys.slice(1);
      
      for (const journeyToRemove of journeysToRemove) {
        try {
          await redis(`del/${journeyToRemove.key}`);
          duplicatesRemoved++;
          console.log(`🗑️  Removed duplicate: ${journeyToRemove.key}`);
        } catch (deleteError) {
          console.warn(`⚠️ Failed to delete ${journeyToRemove.key}: ${deleteError.message}`);
        }
      }
    }
    
    const processingTime = Date.now() - cleanupStartTime;
    console.log(`✅ Cleanup complete: ${duplicatesRemoved} duplicates removed in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        cleanup_complete: true,
        cleanup_summary: {
          keys_scanned: keysScanned,
          unique_order_ids: journeyMap.size,
          duplicates_found: duplicatesFound,
          duplicates_removed: duplicatesRemoved,
          duplicate_order_ids: duplicateOrderIds.length,
          processing_time_ms: processingTime
        },
        remaining_journeys: journeyMap.size,
        next_steps: [
          `Cleanup removed ${duplicatesRemoved} duplicate journey records`,
          `${journeyMap.size} unique journeys remain`,
          'Run analytics query to verify clean dataset'
        ]
      })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Duplicate cleanup failed', message: error.message })
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
