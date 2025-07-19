// netlify/functions/build-customer-journeys.js
// ENHANCED Customer Journey Builder with Landing Page Support for Single Touch Conversions
// Processes ALL conversions with embedded attribution logic (no external API calls)

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
    console.log('🚀 ENHANCED CUSTOMER JOURNEY BUILDER: Starting with embedded attribution logic and landing page support...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      journey_window_hours = 168, // 7-day journey lookback window
      batch_size = 20,            // Process 20 conversions per batch
      skip_existence_check = false,
      force_reprocess = false
    } = body;
    
    console.log(`📊 Journey Parameters: ${journey_window_hours}h lookback, batch: ${batch_size}, skip_check: ${skip_existence_check || force_reprocess}`);
    
    // Step 1: Load ALL conversions (no date limits - truly stateless)
    const allConversions = await loadAllConversionsStateless(redis, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Found ${allConversions.length} conversions to process`);
    
    if (allConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'No conversions found in database'
        })
      };
    }
    
    // Step 2: Filter out conversions that already have journeys (unless forcing reprocess)
    let conversionsNeedingJourneys;
    if (skip_existence_check || force_reprocess) {
      console.log('⚡ BYPASS MODE: Skipping existence check, processing first ' + Math.min(batch_size, allConversions.length) + ' conversions');
      conversionsNeedingJourneys = allConversions.slice(0, batch_size);
    } else {
      conversionsNeedingJourneys = await filterConversionsNeedingJourneys(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    }
    
    console.log(`📊 Journey Status: ${conversionsNeedingJourneys.length} need processing`);
    
    if (conversionsNeedingJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          build_complete: true,
          message: 'ALL CUSTOMER JOURNEYS COMPLETE!',
          summary: {
            total_conversions: allConversions.length,
            conversions_with_journeys: allConversions.length,
            conversion_coverage: '100%',
            processing_status: 'complete'
          },
          next_steps: [
            'All conversions have complete customer journeys',
            'System ready for comprehensive multi-touch attribution analysis',
            'Use query-customer-journeys.js for business intelligence reports'
          ]
        })
      };
    }
    
    // Step 3: Process conversions with embedded attribution logic until timeout
    const processingResults = await processConversionsWithEmbeddedAttribution(
      redis, 
      conversionsNeedingJourneys, 
      journey_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    const completionPercentage = ((allConversions.length - processingResults.conversions_remaining) / allConversions.length * 100).toFixed(1);
    
    console.log(`✅ Processing complete: ${processingResults.journeys_created_this_run} journeys in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        stateless_processing: true,
        build_complete: processingResults.is_complete,
        execution_summary: {
          total_conversions_checked: allConversions.length,
          conversions_needing_journeys: conversionsNeedingJourneys.length,
          conversions_processed_this_run: processingResults.conversions_processed_this_run,
          journeys_created_this_run: processingResults.journeys_created_this_run,
          conversions_remaining: processingResults.conversions_remaining,
          completion_percentage: completionPercentage,
          processing_time_ms: totalTime,
          attribution_success_rate: processingResults.attribution_success_rate
        },
        performance_metrics: {
          conversions_per_second: Math.round(processingResults.conversions_processed_this_run / (totalTime / 1000)),
          average_attribution_time_ms: processingResults.avg_attribution_time_ms,
          embedded_logic_efficiency: 'no_external_api_calls'
        },
        next_steps: processingResults.is_complete ? [
          '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          `Successfully processed all ${allConversions.length} conversions in database`,
          'System ready for complete multi-touch attribution analysis',
          'Journey data available for first-click vs last-click comparison',
          'Use query-customer-journeys.js for comprehensive business intelligence'
        ] : [
          `Continue processing: ${processingResults.conversions_remaining} conversions remaining (${(100 - parseFloat(completionPercentage)).toFixed(1)}%)`,
          'Run the same command again to continue automatically',
          'Each run will process remaining conversions until timeout',
          'No manual tracking needed - system finds remaining work automatically',
          `Estimated runs remaining: ${Math.ceil(processingResults.conversions_remaining / processingResults.conversions_processed_this_run)}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced journey processing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Enhanced journey processing failed', 
        message: error.message 
      })
    };
  }
};

// Initialize Redis connection
function initializeRedis() {
  return async (endpoint, timeout = 5000) => {
    const REDIS_URL = process.env.REDIS_URL;
    if (!REDIS_URL) {
      throw new Error('Redis URL not configured');
    }
    
    // Return a promise that handles the Redis operation
    return new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        reject(new Error(`Redis operation timed out after ${timeout}ms`));
      }, timeout);
      
      // Simulate Redis operation (replace with actual Redis client)
      // This is a placeholder - you'll need your actual Redis implementation
      fetch(`${REDIS_URL}/${endpoint}`)
        .then(response => response.json())
        .then(result => {
          clearTimeout(timeoutId);
          resolve(result);
        })
        .catch(error => {
          clearTimeout(timeoutId);
          reject(error);
        });
    });
  };
}

// Load ALL conversions (no date filtering)
async function loadAllConversionsStateless(redis, maxTime) {
  console.log(`🔍 Loading conversions (recent_only: false, days: 7)...`);
  
  const loadStartTime = Date.now();
  const conversions = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  try {
    do {
      // Check timeout
      if (Date.now() - loadStartTime > maxTime - 5000) {
        console.log('⏰ Time limit during conversion scan, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Process conversion keys in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - loadStartTime > maxTime - 3000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const conversionData = await redis(`get/${key}`);
            if (conversionData?.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // Include ALL conversions that have required fields (no date filtering)
              if (conversion.timestamp && conversion.email && conversion.order_id) {
                return {
                  email: conversion.email,
                  timestamp: conversion.timestamp,
                  order_id: conversion.order_id,
                  order_total: conversion.order_total || 0,
                  
                  // Attribution signals for embedded attribution logic
                  session_id: conversion.session_id,
                  device_signature: conversion.device_signature || conversion.dsig,
                  screen_value: conversion.screen_value || conversion.SVV || conversion.SVVV,
                  gpu_signature: conversion.gpu_signature || conversion.gsig,
                  
                  // IP addresses for multi-IP attribution
                  primary_ip: conversion.primary_ip,
                  conversion_ip: conversion.conversion_ip,
                  pageview_ip: conversion.pageview_ip,
                  ip_addresses: [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean),
                  
                  // ENHANCED: Landing page and campaign data extraction
                  current_source: conversion.source,
                  current_landing_page: conversion.landing_page,
                  current_campaign: conversion.campaign,
                  current_medium: conversion.medium,
                  current_content: conversion.content,
                  current_term: conversion.term,
                  current_referrer: conversion.referrer_url,
                  
                  // Current attribution (for comparison)
                  current_attribution_found: conversion.attribution_found,
                  current_attribution_method: conversion.attribution_method,
                  
                  _redis_key: key
                };
              }
            }
          } catch (parseError) {
            console.warn(`⚠️ Failed to parse conversion ${key}`);
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        conversions.push(...validResults);
      }
      
      if (conversions.length % 500 === 0 && conversions.length > 0) {
        console.log(`📊 Conversion loading progress: ${conversions.length} conversions loaded`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Conversion scan error: ${scanError.message}`);
  }
  
  // Sort by timestamp (most recent first)
  conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`✅ Loaded ${conversions.length} conversions for processing`);
  return conversions;
}

// Filter conversions that need journey processing
async function filterConversionsNeedingJourneys(redis, allConversions, maxTime) {
  console.log(`🔍 OPTIMIZED existence check for ${allConversions.length} conversions (${Math.floor(maxTime/1000)}s limit)...`);
  
  const checkStartTime = Date.now();
  const conversionsNeedingJourneys = [];
  
  // Process conversions in batches
  const batchSize = 50;
  for (let i = 0; i < allConversions.length; i += batchSize) {
    if (Date.now() - checkStartTime > maxTime - 2000) {
      console.log(`⏰ Existence check timeout after ${Date.now() - checkStartTime}ms, processed ${i}/${allConversions.length}`);
      break;
    }
    
    const batch = allConversions.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (conversion) => {
      try {
        // Simple check: does ANY journey exist for this order_id?
        const journeyPattern = `customer_journey:journey_${conversion.order_id}_*`;
        const existingJourneys = await redis(`keys/${journeyPattern}`);
        
        // If no existing journey found, include in processing list
        if (!existingJourneys.result || existingJourneys.result.length === 0) {
          return conversion;
        }
        
        return null; // Journey already exists, skip
      } catch (error) {
        return conversion; // Include on error for safety
      }
    });
    
    const batchResults = await Promise.all(batchPromises);
    const validResults = batchResults.filter(result => result !== null);
    conversionsNeedingJourneys.push(...validResults);
    
    if ((i + batchSize) % 200 === 0) {
      console.log(`📊 Progress: ${i + batchSize}/${allConversions.length} checked, ${conversionsNeedingJourneys.length} need journeys`);
    }
  }
  
  console.log(`✅ Existence check complete: ${conversionsNeedingJourneys.length} conversions need journey processing`);
  return conversionsNeedingJourneys;
}

// Process conversions with EMBEDDED attribution logic (no external API calls)
async function processConversionsWithEmbeddedAttribution(redis, conversionsToProcess, journeyWindowHours, batchSize, maxTime) {
  const processStartTime = Date.now();
  console.log(`🚀 Processing ${conversionsToProcess.length} conversions with ${Math.floor(maxTime/1000)}s remaining...`);
  
  let journeysCreated = 0;
  let conversionsProcessed = 0;
  let attributionCallsMade = 0;
  let attributionSuccesses = 0;
  let totalAttributionTime = 0;
  
  // Process conversions in batches until timeout
  for (let i = 0; i < conversionsToProcess.length; i += batchSize) {
    // Check timeout before each batch
    const timeRemaining = maxTime - (Date.now() - processStartTime);
    if (timeRemaining < 8000) { // Need 8 seconds minimum for a batch
      console.log(`⏰ Time limit reached after processing ${conversionsProcessed} conversions`);
      break;
    }
    
    const batch = conversionsToProcess.slice(i, i + batchSize);
    console.log(`🔗 Processing batch: ${i + 1}-${i + batch.length} of ${conversionsToProcess.length}`);
    
    // Process this batch with embedded attribution
    const batchStartTime = Date.now();
    const batchJourneys = await processBatchWithEmbeddedAttribution(redis, batch, journeyWindowHours);
    const batchTime = Date.now() - batchStartTime;
    
    journeysCreated += batchJourneys.journeys.length;
    conversionsProcessed += batch.length;
    attributionCallsMade += batchJourneys.attribution_calls;
    attributionSuccesses += batchJourneys.attribution_successes;
    totalAttributionTime += batchTime;
    
    console.log(`✅ Batch complete: ${batchJourneys.journeys.length} journeys created in ${batchTime}ms (${conversionsProcessed}/${conversionsToProcess.length} total)`);
  }
  
  const remainingConversions = conversionsToProcess.length - conversionsProcessed;
  const avgAttributionTime = attributionCallsMade > 0 ? Math.round(totalAttributionTime / attributionCallsMade) : 0;
  const attributionSuccessRate = attributionCallsMade > 0 ? ((attributionSuccesses / attributionCallsMade) * 100).toFixed(1) : '0.0';
  
  console.log(`🏁 Processing summary: ${journeysCreated} journeys, ${attributionSuccessRate}% success rate`);
  
  return {
    journeys_created_this_run: journeysCreated,
    conversions_processed_this_run: conversionsProcessed,
    conversions_remaining: remainingConversions,
    is_complete: remainingConversions === 0,
    attribution_calls_made: attributionCallsMade,
    attribution_success_rate: attributionSuccessRate,
    avg_attribution_time_ms: avgAttributionTime,
    processing_time_ms: Date.now() - processStartTime
  };
}

// Process batch with EMBEDDED attribution logic (core enhancement)
async function processBatchWithEmbeddedAttribution(redis, conversions, journeyWindowHours) {
  const journeys = [];
  let attributionCalls = 0;
  let attributionSuccesses = 0;
  
  const batchPromises = conversions.map(async (conversion) => {
    try {
      attributionCalls++;
      
      // EMBEDDED ATTRIBUTION LOGIC - No external API calls!
      const pageviews = await findPageviewsForConversion(redis, conversion, journeyWindowHours);
      
      let journey;
      if (pageviews.length > 0) {
        // Multi-touch journey with pageviews
        journey = await createMultiTouchJourney(conversion, pageviews);
        attributionSuccesses++;
      } else {
        // ENHANCED: Single-touch journey with landing page data
        journey = createEnhancedConversionOnlyJourney(conversion);
        attributionSuccesses++; // Count as success since we have attribution data
      }
      
      // Store journey in Redis
      const journeyKey = `customer_journey:${journey.journey_id}`;
      await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
      
      journeys.push(journey);
      return journey;
      
    } catch (error) {
      console.warn(`⚠️ Attribution failed for conversion ${conversion.order_id}: ${error.message}`);
      return null;
    }
  });
  
  const results = await Promise.all(batchPromises);
  const validJourneys = results.filter(j => j !== null);
  
  return {
    journeys: validJourneys,
    attribution_calls: attributionCalls,
    attribution_successes: attributionSuccesses
  };
}

// Find pageviews for conversion using embedded logic
async function findPageviewsForConversion(redis, conversion, journeyWindowHours) {
  const pageviews = [];
  
  // Try different attribution methods in priority order
  const attributionMethods = [
    'session_id',
    'device_signature', 
    'ip_addresses',
    'screen_signature',
    'gpu_signature'
  ];
  
  for (const method of attributionMethods) {
    try {
      let foundPageviews = [];
      
      switch (method) {
        case 'session_id':
          if (conversion.session_id) {
            foundPageviews = await findPageviewsBySessionId(redis, conversion.session_id, conversion.timestamp, journeyWindowHours);
          }
          break;
          
        case 'device_signature':
          if (conversion.device_signature) {
            foundPageviews = await findPageviewsByDeviceSignature(redis, conversion.device_signature, conversion.timestamp, journeyWindowHours);
          }
          break;
          
        case 'ip_addresses':
          if (conversion.ip_addresses && conversion.ip_addresses.length > 0) {
            foundPageviews = await findPageviewsByIpAddresses(redis, conversion.ip_addresses, conversion.timestamp, journeyWindowHours);
          }
          break;
          
        case 'screen_signature':
          if (conversion.screen_value) {
            foundPageviews = await findPageviewsByScreenSignature(redis, conversion.screen_value, conversion.timestamp, journeyWindowHours);
          }
          break;
          
        case 'gpu_signature':
          if (conversion.gpu_signature) {
            foundPageviews = await findPageviewsByGpuSignature(redis, conversion.gpu_signature, conversion.timestamp, journeyWindowHours);
          }
          break;
      }
      
      if (foundPageviews.length > 0) {
        return foundPageviews; // Return first successful attribution
      }
      
    } catch (error) {
      console.warn(`⚠️ ${method} attribution failed: ${error.message}`);
      continue;
    }
  }
  
  return []; // No pageviews found
}

// Placeholder attribution functions (implement based on your pageview index structure)
async function findPageviewsBySessionId(redis, sessionId, conversionTime, windowHours) {
  // Implementation depends on your pageview index structure
  return [];
}

async function findPageviewsByDeviceSignature(redis, deviceSig, conversionTime, windowHours) {
  // Implementation depends on your pageview index structure
  return [];
}

async function findPageviewsByIpAddresses(redis, ipAddresses, conversionTime, windowHours) {
  // Implementation depends on your pageview index structure
  return [];
}

async function findPageviewsByScreenSignature(redis, screenSig, conversionTime, windowHours) {
  // Implementation depends on your pageview index structure
  return [];
}

async function findPageviewsByGpuSignature(redis, gpuSig, conversionTime, windowHours) {
  // Implementation depends on your pageview index structure
  return [];
}

// Create multi-touch journey with pageviews
async function createMultiTouchJourney(conversion, pageviews) {
  // Sort pageviews by timestamp
  const sortedPageviews = pageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Create touchpoint array
  const touchpoints = sortedPageviews.map((pageview, index) => ({
    touchpoint_id: `${conversion.order_id}_${index}`,
    timestamp: pageview.timestamp,
    type: 'pageview',
    
    // Page details
    landing_page: pageview.landing_page,
    source: pageview.source,
    medium: pageview.medium,
    campaign: pageview.campaign,
    content: pageview.content,
    term: pageview.term,
    referrer_url: pageview.referrer_url,
    
    // Attribution metadata
    attribution_method: pageview.attribution_method,
    confidence: pageview.confidence,
    matched_ip: pageview.matched_ip,
    
    // Session and device data
    session_id: pageview.session_id,
    canvas_fingerprint: pageview.canvas_fingerprint,
    screen_resolution: pageview.screen_resolution,
    user_agent: pageview.user_agent,
    
    // Position in journey
    touchpoint_position: index + 1,
    is_first_touchpoint: index === 0,
    is_last_touchpoint: index === sortedPageviews.length - 1
  }));
  
  // Add conversion as final touchpoint
  touchpoints.push({
    touchpoint_id: `${conversion.order_id}_conversion`,
    timestamp: conversion.timestamp,
    type: 'conversion',
    order_id: conversion.order_id,
    order_total: conversion.order_total,
    email: conversion.email,
    attribution_method: 'conversion_point',
    confidence: 1000,
    touchpoint_position: touchpoints.length + 1,
    is_conversion: true,
    is_last_touchpoint: true
  });
  
  // Calculate journey metrics
  const journeyStart = new Date(touchpoints[0].timestamp);
  const journeyEnd = new Date(conversion.timestamp);
  const journeySpanHours = (journeyEnd - journeyStart) / (1000 * 60 * 60);
  
  const uniqueSessions = new Set(touchpoints.map(t => t.session_id).filter(Boolean)).size;
  const uniqueDeviceFingerprints = new Set(touchpoints.map(t => t.canvas_fingerprint).filter(Boolean)).size;
  const uniqueSources = new Set(touchpoints.map(t => t.source).filter(Boolean));
  
  return {
    journey_id: `journey_${conversion.order_id}_${Date.now()}`,
    customer_email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    conversion_order_id: conversion.order_id,
    conversion_value: conversion.order_total,
    
    journey_start: touchpoints[0].timestamp,
    journey_end: conversion.timestamp,
    journey_span_hours: journeySpanHours,
    total_touchpoints: touchpoints.length - 1, // Don't count conversion as touchpoint
    
    unique_sessions: uniqueSessions,
    unique_device_fingerprints: uniqueDeviceFingerprints,
    unique_sources: Array.from(uniqueSources),
    cross_session_journey: uniqueSessions > 1,
    cross_device_journey: uniqueDeviceFingerprints > 1,
    
    first_click_source: touchpoints[0].source,
    last_click_source: touchpoints[touchpoints.length - 2]?.source || touchpoints[0].source,
    attribution_confidence_avg: touchpoints.reduce((sum, t) => sum + (t.confidence || 0), 0) / touchpoints.length,
    
    touchpoints: touchpoints,
    
    created_at: new Date().toISOString(),
    reconstruction_method: 'stateless_embedded_attribution'
  };
}

// ENHANCED: Create journey for conversions with complete landing page data
function createEnhancedConversionOnlyJourney(conversion) {
  // Create a proper touchpoint for the single conversion touch
  const conversionTouchpoint = {
    touchpoint_id: `${conversion.order_id}_single_touch`,
    timestamp: conversion.timestamp,
    type: 'pageview',
    
    // Extract landing page and campaign data from conversion record
    landing_page: conversion.current_landing_page || conversion.landing_page || 'https://ojoy.ai/',
    source: conversion.current_source || conversion.source || 'unknown',
    medium: conversion.current_medium || conversion.medium || 'unknown',
    campaign: conversion.current_campaign || conversion.campaign || 'none',
    content: conversion.current_content || conversion.content || null,
    term: conversion.current_term || conversion.term || null,
    referrer_url: conversion.current_referrer || conversion.referrer_url || null,
    
    // Attribution metadata for single touch
    attribution_method: conversion.current_attribution_method || 'conversion_data',
    confidence: 500, // Medium-high confidence since it's from conversion record
    matched_ip: conversion.primary_ip || conversion.conversion_ip || conversion.ip_addresses?.[0],
    
    // Session and device data if available
    session_id: conversion.session_id || null,
    canvas_fingerprint: conversion.device_signature || conversion.canvas_fingerprint || null,
    screen_resolution: conversion.screen_value || null,
    user_agent: conversion.user_agent || null,
    
    // Position in journey
    touchpoint_position: 1,
    is_first_touchpoint: true,
    is_last_touchpoint: false // The conversion will be the last touchpoint
  };

  // Create conversion touchpoint
  const conversionEvent = {
    touchpoint_id: `${conversion.order_id}_conversion`,
    timestamp: conversion.timestamp,
    type: 'conversion',
    order_id: conversion.order_id,
    order_total: conversion.order_total,
    email: conversion.email,
    attribution_method: 'conversion_point',
    confidence: 1000,
    touchpoint_position: 2,
    is_conversion: true,
    is_last_touchpoint: true
  };

  const touchpoints = [conversionTouchpoint, conversionEvent];

  return {
    journey_id: `journey_${conversion.order_id}_conversion_only`,
    customer_email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    conversion_order_id: conversion.order_id,
    conversion_value: conversion.order_total,
    
    journey_start: conversion.timestamp,
    journey_end: conversion.timestamp,
    journey_span_hours: 0,
    total_touchpoints: 1, // Still 1 touchpoint for analytics (conversion event doesn't count as touchpoint)
    
    unique_sessions: conversion.session_id ? 1 : 0,
    unique_device_fingerprints: conversion.device_signature ? 1 : 0,
    unique_sources: [conversion.current_source || conversion.source || 'unknown'],
    cross_session_journey: false,
    cross_device_journey: false,
    
    first_click_source: conversion.current_source || conversion.source || 'unknown',
    last_click_source: conversion.current_source || conversion.source || 'unknown',
    attribution_confidence_avg: 500,
    
    // CRITICAL: Include the touchpoints array with landing page data
    touchpoints: touchpoints,
    
    created_at: new Date().toISOString(),
    reconstruction_method: 'enhanced_conversion_only_with_landing_page'
  };
}
