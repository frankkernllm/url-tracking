// netlify/functions/query-customer-journeys.js
// UPDATED: True Cursor-Based Pagination - Complete Script
// This eliminates the Bobby journey issue and provides unlimited scalability

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
    console.log('🔍 CURSOR-BASED JOURNEY QUERY: Starting true pagination...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters with new cursor support
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      // NEW: Actual date range support
      start_date = null,
      end_date = null,
      // OLD: Backward compatibility
      date_range_days = null,
      limit = 100,
      // NEW: Cursor-based pagination
      cursor = '0',
      // OLD: Backward compatibility for offset-based calls
      offset = 0,
      sample_only = false,
      cleanup_duplicates = false,
      analytics_mode = false,
      journey_id = null,
      customer_email = null,
      order_id = null
    } = body;
    
    // FIXED: Calculate date filtering parameters properly
    let cutoffTimestamp;
    let endTimestamp;
    let dateFilterDescription;
    
    if (start_date && end_date) {
      // NEW: Use actual date range from dashboard
      const startDateObj = new Date(start_date);
      const endDateObj = new Date(end_date);
      endDateObj.setHours(23, 59, 59, 999); // Include full end date
      
      cutoffTimestamp = startDateObj.getTime();
      endTimestamp = endDateObj.getTime();
      dateFilterDescription = `${start_date} to ${end_date}`;
      
      console.log(`📅 Date Range (NEW): ${start_date} to ${end_date}`);
    } else if (date_range_days) {
      // OLD: Backward compatibility - relative to current date
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - date_range_days);
      cutoffTimestamp = cutoffDate.getTime();
      endTimestamp = Date.now();
      dateFilterDescription = `${date_range_days} days from today`;
      
      console.log(`📅 Relative Range (OLD): ${date_range_days} days from today`);
    } else {
      // Default: Last 7 days
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - 7);
      cutoffTimestamp = cutoffDate.getTime();
      endTimestamp = Date.now();
      dateFilterDescription = `Last 7 days (default)`;
      
      console.log(`📅 Default Range: Last 7 days`);
    }
    
    console.log(`📊 Query Parameters: ${dateFilterDescription}, limit: ${limit}, cursor: ${cursor}`);
    
    // Handle different query modes
    if (cleanup_duplicates) {
      return await handleDuplicateCleanup(redis, maxProcessingTime - (Date.now() - startTime));
    }
    
    if (sample_only) {
      return await handleSampleQuery(redis, limit, maxProcessingTime - (Date.now() - startTime));
    }
    
    if (analytics_mode) {
      return await handleAnalyticsQuery(redis, cutoffTimestamp, endTimestamp, maxProcessingTime - (Date.now() - startTime));
    }
    
    // UPDATED: Cursor-based journey search
    const searchResults = await performEfficientJourneySearch(redis, {
      cutoffTimestamp,
      endTimestamp,
      limit,
      cursor, // NEW: Pass cursor instead of offset
      journey_id,
      customer_email,
      order_id
    }, maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Cursor-based query complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        query_optimized: true,
        pagination_type: 'cursor_based', // NEW: Indicate pagination type
        query_results: searchResults.journeys,
        date_filter: {
          description: dateFilterDescription,
          start_timestamp: cutoffTimestamp,
          end_timestamp: endTimestamp,
          start_date: start_date,
          end_date: end_date
        },
        query_summary: {
          journeys_found: searchResults.journeys_found,
          journeys_returned: searchResults.journeys.length,
          keys_scanned: searchResults.keys_scanned,
          processing_time_ms: totalTime,
          has_more_results: searchResults.has_more_results
        },
        performance: {
          journeys_per_second: Math.round(searchResults.journeys_found / (totalTime / 1000)),
          scanning_efficiency: 'redis_cursor_native',
          memory_efficient: true
        },
        // NEW: Cursor-based pagination info
        pagination: {
          current_limit: limit,
          current_cursor: cursor,
          next_cursor: searchResults.next_cursor,
          has_more_results: searchResults.has_more_results,
          // NEW: Instructions for next call
          next_call_example: searchResults.has_more_results ? {
            cursor: searchResults.next_cursor,
            limit: limit,
            start_date: start_date,
            end_date: end_date
          } : null,
          // OLD: Backward compatibility
          legacy_offset_equivalent: offset + searchResults.journeys.length
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Cursor-based journey query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Cursor-based journey query failed', 
        message: error.message 
      })
    };
  }
};

// UPDATED: True cursor-based pagination (no iteration limits!)
async function performEfficientJourneySearch(redis, searchParams, maxTime) {
  const searchStartTime = Date.now();
  const { 
    cutoffTimestamp, 
    endTimestamp, 
    limit, 
    cursor = '0', // NEW: Accept cursor instead of offset
    journey_id, 
    customer_email, 
    order_id 
  } = searchParams;
  
  console.log(`🔍 True Cursor Search: Starting from cursor ${cursor}, limit: ${limit}`);
  
  const journeys = [];
  let keysScanned = 0;
  let journeysFound = 0;
  let currentCursor = cursor;
  let iterations = 0;
  
  try {
    do {
      // Check timeout
      if (Date.now() - searchStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during journey search, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${currentCursor}/match/customer_journey:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log('🏁 Scan complete: no more Redis keys');
        currentCursor = '0'; // Mark as complete
        break;
      }
      
      currentCursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Process keys in batches
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - searchStartTime > maxTime - 2000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Apply all filters
              if (passesDateAndFilters(journey, cutoffTimestamp, endTimestamp, journey_id, customer_email, order_id)) {
                return journey;
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(j => j !== null);
        
        for (const journey of validJourneys) {
          journeysFound++;
          
          // Stop if we've collected enough journeys
          if (journeys.length >= limit) {
            console.log(`✅ Limit reached: ${journeys.length} journeys collected`);
            return {
              journeys: journeys,
              journeys_found: journeysFound,
              keys_scanned: keysScanned,
              has_more_results: true,
              next_cursor: currentCursor,
              processing_time_ms: Date.now() - searchStartTime
            };
          }
          
          journeys.push(journey);
        }
        
        if (journeys.length >= limit) break;
      }
      
      if (journeys.length >= limit) break;
      
      if (iterations % 10 === 0) {
        console.log(`🔍 Search progress: ${keysScanned} keys scanned, ${journeysFound} journeys found, ${journeys.length} returned`);
      }
      
    } while (currentCursor !== '0' && journeys.length < limit);
    
    // Determine if there are more results
    const hasMoreResults = currentCursor !== '0';
    
    console.log(`✅ Cursor search complete: ${journeys.length} journeys returned from ${journeysFound} found`);
    console.log(`📍 Final cursor position: ${currentCursor} (has_more: ${hasMoreResults})`);
    
    return {
      journeys: journeys,
      journeys_found: journeysFound,
      keys_scanned: keysScanned,
      has_more_results: hasMoreResults,
      next_cursor: hasMoreResults ? currentCursor : null,
      processing_time_ms: Date.now() - searchStartTime
    };
    
  } catch (error) {
    console.error('❌ Cursor-based journey search error:', error);
    return {
      journeys: journeys,
      journeys_found: journeysFound,
      keys_scanned: keysScanned,
      has_more_results: false,
      next_cursor: null,
      error: error.message
    };
  }
}

// FIXED: Filter function with proper date range filtering
function passesDateAndFilters(journey, cutoffTimestamp, endTimestamp, journey_id, customer_email, order_id) {
  // FIXED: Date filter - check if conversion is within the specified range
  if (journey.conversion_timestamp) {
    const journeyTime = new Date(journey.conversion_timestamp).getTime();
    if (journeyTime < cutoffTimestamp || journeyTime > endTimestamp) {
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
              journey_span_hours: journey.journey_span_hours
            });
          }
        } catch (parseError) {
          // Skip invalid data
        }
        
        if (samples.length >= limit) break;
      }
      
      if (samples.length >= limit) break;
      
    } while (cursor !== '0');
    
    const totalTime = Date.now() - sampleStartTime;
    console.log(`✅ Sample query complete: ${samples.length} journeys in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        sample_mode: true,
        query_results: samples,
        query_summary: {
          journeys_returned: samples.length,
          keys_scanned: keysScanned,
          processing_time_ms: totalTime,
          sample_query: true
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

// ANALYTICS: Comprehensive journey analytics
async function handleAnalyticsQuery(redis, cutoffTimestamp, endTimestamp, maxTime) {
  console.log('📊 Analytics mode: Comprehensive journey analysis...');
  
  const analyticsStartTime = Date.now();
  let cursor = '0';
  let iterations = 0;
  let keysScanned = 0;
  
  const analytics = {
    total_journeys: 0,
    total_conversion_value: 0,
    total_touchpoints: 0,
    multi_touchpoint_journeys: 0,
    cross_session_journeys: 0,
    cross_device_journeys: 0,
    attribution_sources: {},
    journey_lengths: {},
    conversion_hours: {}
  };
  
  try {
    do {
      if (Date.now() - analyticsStartTime > maxTime - 3000) {
        console.log('⏰ Analytics timeout, stopping');
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
      
      // Process analytics in batches
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - analyticsStartTime > maxTime - 2000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              if (passesDateAndFilters(journey, cutoffTimestamp, endTimestamp, null, null, null)) {
                return journey;
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(j => j !== null);
        
        // Process analytics for valid journeys
        for (const journey of validJourneys) {
          analytics.total_journeys++;
          analytics.total_conversion_value += journey.conversion_value || 0;
          analytics.total_touchpoints += journey.total_touchpoints || 0;
          
          if ((journey.total_touchpoints || 0) > 1) {
            analytics.multi_touchpoint_journeys++;
          }
          
          if (journey.cross_session) {
            analytics.cross_session_journeys++;
          }
          
          if (journey.cross_device) {
            analytics.cross_device_journeys++;
          }
          
          // Source attribution
          const firstSource = journey.first_click_source || 'unknown';
          analytics.attribution_sources[firstSource] = (analytics.attribution_sources[firstSource] || 0) + 1;
          
          // Journey length distribution
          const touchpoints = journey.total_touchpoints || 0;
          const lengthBucket = touchpoints <= 1 ? '1' : touchpoints <= 3 ? '2-3' : touchpoints <= 5 ? '4-5' : '6+';
          analytics.journey_lengths[lengthBucket] = (analytics.journey_lengths[lengthBucket] || 0) + 1;
          
          // Conversion time analysis
          if (journey.conversion_timestamp) {
            const hour = new Date(journey.conversion_timestamp).getHours();
            analytics.conversion_hours[hour] = (analytics.conversion_hours[hour] || 0) + 1;
          }
        }
      }
      
      if (iterations % 5 === 0) {
        console.log(`📊 Analytics progress: ${analytics.total_journeys} journeys processed`);
      }
      
    } while (cursor !== '0');
    
    const processingTime = Date.now() - analyticsStartTime;
    console.log(`✅ Analytics complete: ${analytics.total_journeys} journeys in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        analytics_mode: true,
        processing_summary: {
          iterations_completed: iterations,
          keys_scanned: keysScanned,
          processing_time_ms: processingTime,
          journeys_per_second: Math.round(analytics.total_journeys / (processingTime / 1000))
        },
        journey_analytics: {
          total_journeys: analytics.total_journeys,
          total_conversion_value: analytics.total_conversion_value,
          average_conversion_value: analytics.total_journeys > 0 ? 
            (analytics.total_conversion_value / analytics.total_journeys).toFixed(2) : 0,
          multi_touchpoint_rate: analytics.total_journeys > 0 ? 
            ((analytics.multi_touchpoint_journeys / analytics.total_journeys) * 100).toFixed(1) : 0,
          cross_session_rate: analytics.total_journeys > 0 ? 
            ((analytics.cross_session_journeys / analytics.total_journeys) * 100).toFixed(1) : 0,
          cross_device_rate: analytics.total_journeys > 0 ? 
            ((analytics.cross_device_journeys / analytics.total_journeys) * 100).toFixed(1) : 0
        },
        attribution_breakdown: analytics.attribution_sources,
        journey_length_distribution: analytics.journey_lengths,
        conversion_hour_distribution: analytics.conversion_hours
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

// OPTIMIZED: Duplicate cleanup
async function handleDuplicateCleanup(redis, maxTime) {
  console.log('🧹 Starting duplicate cleanup process...');
  
  const cleanupStartTime = Date.now();
  let cursor = '0';
  let keysScanned = 0;
  let duplicatesFound = 0;
  let duplicatesRemoved = 0;
  const journeyMap = new Map(); // order_id -> journey_key
  const duplicateOrderIds = [];
  
  try {
    // Scan all journey keys
    do {
      if (Date.now() - cleanupStartTime > maxTime - 5000) break;
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      
      // Check each key for duplicates
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - cleanupStartTime > maxTime - 3000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              const orderId = journey.conversion_order_id;
              
              if (orderId) {
                if (journeyMap.has(orderId)) {
                  duplicateOrderIds.push(orderId);
                  return { key, orderId, isDuplicate: true };
                } else {
                  journeyMap.set(orderId, key);
                  return { key, orderId, isDuplicate: false };
                }
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const duplicates = batchResults.filter(r => r && r.isDuplicate);
        duplicatesFound += duplicates.length;
      }
      
    } while (cursor !== '0');
    
    // Remove duplicates
    for (const orderId of duplicateOrderIds) {
      if (Date.now() - cleanupStartTime > maxTime - 2000) break;
      
      try {
        const duplicateKey = journeyMap.get(orderId);
        if (duplicateKey) {
          await redis(`del/${duplicateKey}`);
          duplicatesRemoved++;
        }
      } catch (deleteError) {
        console.error(`Failed to delete duplicate ${orderId}:`, deleteError);
      }
    }
    
    const totalTime = Date.now() - cleanupStartTime;
    console.log(`✅ Cleanup complete: ${duplicatesRemoved} duplicates removed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        cleanup_mode: true,
        cleanup_summary: {
          keys_scanned: keysScanned,
          duplicates_found: duplicatesFound,
          duplicates_removed: duplicatesRemoved,
          processing_time_ms: totalTime
        }
      })
    };
    
  } catch (error) {
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Cleanup failed', message: error.message })
    };
  }
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 1500) => {
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
