// netlify/functions/query-customer-journeys.js
// FIXED: Source attribution breakdown now reads from journey.pageviews[0].source
// BUG: Was looking for journey.first_click_source (doesn't exist)
// SOLUTION: Extract source from first pageview in each journey

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
    const startTime = Date.now();
    const maxTime = 23000; // 23 seconds max to avoid timeout
    
    const redis = initializeRedis();
    
    // Parse request body
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      analytics_mode = false,
      sample_only = false,
      cleanup_duplicates = false,
      customer_email,
      limit = sample_only ? 5 : 50,
      cursor = '0',
      date_range_days = 7
    } = body;

    console.log(`🔍 Journey Query: analytics=${analytics_mode}, sample=${sample_only}, limit=${limit}, date_range=${date_range_days}d`);

    // Calculate date filter
    const endTimestamp = Date.now();
    const cutoffTimestamp = endTimestamp - (date_range_days * 24 * 60 * 60 * 1000);

    // ANALYTICS MODE: Comprehensive business intelligence
    if (analytics_mode) {
      return await processAnalyticsMode(redis, cutoffTimestamp, endTimestamp, maxTime - (Date.now() - startTime));
    }

    // CLEANUP MODE: Remove duplicate journeys
    if (cleanup_duplicates) {
      return await processCleanupMode(redis, maxTime - (Date.now() - startTime));
    }

    // STANDARD MODE: Query journeys with pagination
    return await processStandardQuery(redis, {
      customer_email,
      limit,
      cursor,
      cutoffTimestamp,
      endTimestamp,
      sample_only
    }, maxTime - (Date.now() - startTime));

  } catch (error) {
    console.error('❌ Journey query failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: error.message })
    };
  }
};

// FIXED: Analytics mode with correct source attribution
async function processAnalyticsMode(redis, cutoffTimestamp, endTimestamp, maxTime) {
  console.log('📊 Analytics Mode: Processing journey analytics...');
  const analyticsStartTime = Date.now();
  
  // Initialize analytics structure
  const analytics = {
    total_journeys: 0,
    multi_touchpoint_journeys: 0,
    single_touchpoint_journeys: 0,
    conversion_only_journeys: 0,
    cross_session_journeys: 0,
    cross_device_journeys: 0,
    total_conversion_value: 0,
    unique_customers: new Set(),
    sources: {}, // FIXED: This will now be properly populated
    journey_span_distribution: {
      under_1h: 0,
      '1-24h': 0,
      '1-7d': 0,
      over_7d: 0
    },
    touchpoint_distribution: {
      single: 0,
      '2-3': 0,
      '4-10': 0,
      over_10: 0
    },
    conversion_hour_distribution: {}
  };

  let cursor = '0';
  let iterations = 0;
  const maxIterations = 50;
  
  do {
    if (Date.now() - analyticsStartTime > maxTime - 3000) {
      console.log('⏰ Analytics timeout protection triggered');
      break;
    }

    const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
    
    if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
      break;
    }
    
    cursor = scanResult.result[0];
    const keys = scanResult.result[1] || [];
    iterations++;
    
    // Process journeys in batches
    const batchSize = 20;
    for (let i = 0; i < keys.length; i += batchSize) {
      if (Date.now() - analyticsStartTime > maxTime - 2000) break;
      
      const batch = keys.slice(i, i + batchSize);
      const batchPromises = batch.map(async (key) => {
        try {
          const journeyData = await redis(`get/${key}`, 800);
          if (journeyData?.result) {
            const journey = JSON.parse(decodeURIComponent(journeyData.result));
            
            // Apply date filtering
            if (journey.conversion_timestamp) {
              const journeyTime = new Date(journey.conversion_timestamp).getTime();
              if (journeyTime >= cutoffTimestamp && journeyTime <= endTimestamp) {
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
      const validJourneys = batchResults.filter(j => j !== null);
      
      // FIXED: Analytics aggregation with correct source extraction
      for (const journey of validJourneys) {
        analytics.total_journeys++;
        
        // Touchpoint classification
        if (journey.total_touchpoints > 1) analytics.multi_touchpoint_journeys++;
        else if (journey.total_touchpoints === 1) analytics.single_touchpoint_journeys++;
        else analytics.conversion_only_journeys++;
        
        // Cross-session and cross-device detection
        if (journey.cross_session_journey) analytics.cross_session_journeys++;
        if (journey.cross_device_journey) analytics.cross_device_journeys++;
        
        // Value aggregation
        analytics.total_conversion_value += parseFloat(journey.conversion_value || 0);
        
        // Customer tracking
        if (journey.customer_email && journey.customer_email !== 'unknown') {
          analytics.unique_customers.add(journey.customer_email);
        }
        
        // FIXED: Source aggregation - read from pageviews array
        let attributionSource = 'unknown';
        
        if (journey.pageviews && journey.pageviews.length > 0) {
          // Get first-touch attribution source
          const firstPageview = journey.pageviews[0];
          attributionSource = firstPageview.source || 'unknown';
          
          // Source classification for better analytics
          if (attributionSource === 'direct_typed') attributionSource = 'direct';
          if (attributionSource === 'email_new') attributionSource = 'email';
          if (attributionSource.includes('google')) attributionSource = 'google';
          if (attributionSource.includes('fb') || attributionSource.includes('facebook')) attributionSource = 'facebook';
          if (attributionSource.includes('utm')) attributionSource = 'campaign';
        } else {
          // For conversion-only journeys (no pageviews)
          attributionSource = 'direct';
        }
        
        // Aggregate source counts
        analytics.sources[attributionSource] = (analytics.sources[attributionSource] || 0) + 1;
        
        // Journey span distribution
        const journeySpanHours = calculateJourneySpan(journey);
        if (journeySpanHours < 1) analytics.journey_span_distribution.under_1h++;
        else if (journeySpanHours < 24) analytics.journey_span_distribution['1-24h']++;
        else if (journeySpanHours < 168) analytics.journey_span_distribution['1-7d']++;
        else analytics.journey_span_distribution.over_7d++;
        
        // Touchpoint distribution
        const touchpoints = journey.total_touchpoints || 1;
        if (touchpoints === 1) analytics.touchpoint_distribution.single++;
        else if (touchpoints <= 3) analytics.touchpoint_distribution['2-3']++;
        else if (touchpoints <= 10) analytics.touchpoint_distribution['4-10']++;
        else analytics.touchpoint_distribution.over_10++;
        
        // Conversion hour distribution
        if (journey.conversion_timestamp) {
          const hour = new Date(journey.conversion_timestamp).getHours();
          analytics.conversion_hour_distribution[hour] = (analytics.conversion_hour_distribution[hour] || 0) + 1;
        }
      }
    }
    
    if (iterations % 3 === 0) {
      console.log(`📈 Analytics progress: ${analytics.total_journeys} journeys processed`);
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  // Finalize analytics
  const uniqueCustomerCount = analytics.unique_customers.size;
  delete analytics.unique_customers; // Remove Set for JSON serialization
  
  // Sort attribution sources by count
  const sortedSources = Object.entries(analytics.sources)
    .sort(([,a], [,b]) => b - a)
    .reduce((obj, [key, value]) => {
      obj[key] = value;
      return obj;
    }, {});
  
  const processingTime = Date.now() - analyticsStartTime;
  console.log(`✅ FIXED Analytics complete: ${analytics.total_journeys} journeys, ${Object.keys(sortedSources).length} sources found in ${processingTime}ms`);
  
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      success: true,
      analytics_mode: true,
      source_attribution_fixed: true,
      processing_summary: {
        iterations_completed: iterations,
        keys_scanned: iterations * 100,
        processing_time_ms: processingTime,
        journeys_per_second: Math.round(analytics.total_journeys / (processingTime / 1000))
      },
      journey_analytics: {
        total_journeys: analytics.total_journeys,
        total_conversion_value: analytics.total_conversion_value,
        average_conversion_value: analytics.total_journeys > 0 ? 
          (analytics.total_conversion_value / analytics.total_journeys).toFixed(2) : "0.00",
        multi_touchpoint_rate: analytics.total_journeys > 0 ? 
          ((analytics.multi_touchpoint_journeys / analytics.total_journeys) * 100).toFixed(1) : "0.0",
        cross_session_rate: analytics.total_journeys > 0 ? 
          ((analytics.cross_session_journeys / analytics.total_journeys) * 100).toFixed(1) : "0.0",
        cross_device_rate: analytics.total_journeys > 0 ? 
          ((analytics.cross_device_journeys / analytics.total_journeys) * 100).toFixed(1) : "0.0",
        unique_customers: uniqueCustomerCount
      },
      attribution_breakdown: sortedSources,
      journey_length_distribution: analytics.touchpoint_distribution,
      conversion_hour_distribution: analytics.conversion_hour_distribution,
      date_filter: {
        description: `${(Date.now() - cutoffTimestamp) / (24 * 60 * 60 * 1000)} days from today`,
        start_timestamp: cutoffTimestamp,
        end_timestamp: endTimestamp
      }
    })
  };
}

// Helper function to calculate journey span
function calculateJourneySpan(journey) {
  if (!journey.pageviews || journey.pageviews.length === 0) return 0;
  
  const conversionTime = new Date(journey.conversion_timestamp).getTime();
  const firstPageviewTime = new Date(journey.pageviews[0].timestamp).getTime();
  
  return Math.abs(conversionTime - firstPageviewTime) / (1000 * 60 * 60); // Hours
}

// Standard query processing
async function processStandardQuery(redis, params, maxTime) {
  const { customer_email, limit, cursor, cutoffTimestamp, endTimestamp, sample_only } = params;
  const queryStartTime = Date.now();
  
  console.log(`🔍 Standard Query: limit=${limit}, sample_only=${sample_only}`);
  
  const journeys = [];
  let currentCursor = cursor;
  let iterations = 0;
  const maxIterations = 30;
  
  do {
    if (Date.now() - queryStartTime > maxTime - 1000) {
      console.log('⏰ Query timeout protection triggered');
      break;
    }

    const scanResult = await redis(`scan/${currentCursor}/match/customer_journey:*/count/50`);
    
    if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
      break;
    }
    
    currentCursor = scanResult.result[0];
    const keys = scanResult.result[1] || [];
    iterations++;
    
    for (const key of keys) {
      if (journeys.length >= limit) break;
      if (Date.now() - queryStartTime > maxTime - 500) break;
      
      try {
        const journeyData = await redis(`get/${key}`, 500);
        if (journeyData?.result) {
          const journey = JSON.parse(decodeURIComponent(journeyData.result));
          
          // Apply filters
          let include = true;
          
          // Date filter
          if (journey.conversion_timestamp) {
            const journeyTime = new Date(journey.conversion_timestamp).getTime();
            if (journeyTime < cutoffTimestamp || journeyTime > endTimestamp) {
              include = false;
            }
          }
          
          // Customer email filter
          if (customer_email && journey.customer_email !== customer_email) {
            include = false;
          }
          
          if (include) {
            journeys.push(journey);
          }
        }
      } catch (parseError) {
        // Skip invalid data
      }
    }
    
  } while (currentCursor !== '0' && journeys.length < limit && iterations < maxIterations);
  
  const processingTime = Date.now() - queryStartTime;
  console.log(`✅ Standard query complete: ${journeys.length} journeys in ${processingTime}ms`);
  
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      success: true,
      query_optimized: true,
      pagination_type: 'cursor_based',
      query_results: journeys,
      date_filter: {
        description: `${Math.round((Date.now() - cutoffTimestamp) / (24 * 60 * 60 * 1000))} days from today`,
        start_timestamp: cutoffTimestamp,
        end_timestamp: endTimestamp,
        start_date: null,
        end_date: null
      },
      query_summary: {
        journeys_found: journeys.length,
        journeys_returned: journeys.length,
        keys_scanned: iterations * 50,
        processing_time_ms: processingTime,
        has_more_results: currentCursor !== '0'
      },
      performance: {
        journeys_per_second: Math.round(journeys.length / (processingTime / 1000)),
        scanning_efficiency: 'redis_cursor_native',
        memory_efficient: true
      },
      pagination: {
        current_limit: limit,
        current_cursor: cursor,
        next_cursor: currentCursor,
        has_more_results: currentCursor !== '0',
        next_call_example: {
          cursor: currentCursor,
          limit: limit,
          start_date: null,
          end_date: null
        },
        legacy_offset_equivalent: journeys.length
      }
    })
  };
}

// Cleanup mode processing
async function processCleanupMode(redis, maxTime) {
  console.log('🧹 Cleanup Mode: Removing duplicate journeys...');
  const cleanupStartTime = Date.now();
  
  const seenOrderIds = new Set();
  const duplicateKeys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 50;
  
  do {
    if (Date.now() - cleanupStartTime > maxTime - 3000) {
      console.log('⏰ Cleanup timeout protection triggered');
      break;
    }

    const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
    
    if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
      break;
    }
    
    cursor = scanResult.result[0];
    const keys = scanResult.result[1] || [];
    iterations++;
    
    for (const key of keys) {
      if (Date.now() - cleanupStartTime > maxTime - 2000) break;
      
      try {
        const journeyData = await redis(`get/${key}`, 500);
        if (journeyData?.result) {
          const journey = JSON.parse(decodeURIComponent(journeyData.result));
          const orderId = journey.conversion_order_id;
          
          if (seenOrderIds.has(orderId)) {
            duplicateKeys.push(key);
          } else {
            seenOrderIds.add(orderId);
          }
        }
      } catch (parseError) {
        // Skip invalid data
      }
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  // Delete duplicates in batches
  let deletedCount = 0;
  const batchSize = 10;
  for (let i = 0; i < duplicateKeys.length; i += batchSize) {
    if (Date.now() - cleanupStartTime > maxTime - 1000) break;
    
    const batch = duplicateKeys.slice(i, i + batchSize);
    const deletePromises = batch.map(key => redis(`del/${key}`, 500));
    
    try {
      await Promise.all(deletePromises);
      deletedCount += batch.length;
    } catch (deleteError) {
      console.warn('⚠️ Some deletes failed:', deleteError.message);
    }
  }
  
  const processingTime = Date.now() - cleanupStartTime;
  console.log(`✅ Cleanup complete: ${deletedCount} duplicates removed in ${processingTime}ms`);
  
  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({
      success: true,
      cleanup_completed: true,
      duplicates_removed: deletedCount,
      unique_journeys_remaining: seenOrderIds.size,
      processing_time_ms: processingTime
    })
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
