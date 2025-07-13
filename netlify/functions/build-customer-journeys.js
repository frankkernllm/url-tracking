// netlify/functions/build-customer-journeys.js
// STATELESS Customer Journey Reconstruction Engine
// Pattern: Same proven approach as extract-conversions-chunked-enhanced.js
// Processes ALL conversions in database, naturally resumable, no progress corruption

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

  try {
    console.log('🚀 STATELESS CUSTOMER JOURNEY BUILDER: Starting...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    // Validate required environment variables
    if (!process.env.OJOY_API_KEY) {
      return {
        statusCode: 500,
        headers,
        body: JSON.stringify({
          error: 'Configuration error',
          message: 'OJOY_API_KEY environment variable not set'
        })
      };
    }
    
    const redis = initializeRedis();
    const queryEnhancedAttribution = createEnhancedAttributionQuerier();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const { journey_window_hours = 168 } = body; // 7-day journey lookback window
    
    console.log(`📊 Parameters: ${journey_window_hours}h journey window`);
    
    // Step 1: Load ALL conversions (no date limits - scan entire database)
    const allConversions = await loadAllConversionsForJourneyBuilding(redis, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Found ${allConversions.length} total conversions in database`);
    
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
    
    // Step 2: Filter out conversions that already have journeys (stateless check)
    const conversionsNeedingJourneys = await filterConversionsNeedingJourneys(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    console.log(`📊 Journey Status: ${conversionsNeedingJourneys.length} need processing, ${allConversions.length - conversionsNeedingJourneys.length} already complete`);
    
    if (conversionsNeedingJourneys.length === 0) {
      // Update final analytics
      await updateJourneyAnalytics(redis, allConversions.length);
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'ALL CUSTOMER JOURNEYS COMPLETE!',
          completion_summary: {
            total_conversions_in_database: allConversions.length,
            conversions_with_journeys: allConversions.length,
            build_status: 'complete',
            completion_percentage: '100.0%'
          },
          next_steps: [
            '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
            `Successfully processed all ${allConversions.length} conversions in database`,
            'System ready for complete multi-touch attribution analysis',
            'Use query-customer-journeys.js for comprehensive business intelligence'
          ]
        })
      };
    }
    
    // Step 3: Process conversions until timeout (stateless batch processing)
    const processingResults = await processConversionsUntilTimeout(
      redis, 
      conversionsNeedingJourneys, 
      queryEnhancedAttribution, 
      journey_window_hours,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Stateless processing complete: ${processingResults.journeys_created_this_run} journeys in ${totalTime}ms`);
    
    // Update analytics if complete
    if (processingResults.is_complete) {
      await updateJourneyAnalytics(redis, allConversions.length);
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        stateless_processing: true,
        build_complete: processingResults.is_complete,
        execution_summary: {
          total_conversions_in_database: allConversions.length,
          conversions_needing_journeys_at_start: conversionsNeedingJourneys.length,
          conversions_processed_this_run: processingResults.conversions_processed_this_run,
          journeys_created_this_run: processingResults.journeys_created_this_run,
          conversions_remaining: processingResults.conversions_remaining,
          processing_time_ms: totalTime,
          completion_percentage: (((allConversions.length - processingResults.conversions_remaining) / allConversions.length) * 100).toFixed(1),
          efficiency: `${processingResults.conversions_processed_this_run} conversions in ${Math.round(totalTime/1000)} seconds`
        },
        journey_quality_metrics: processingResults.quality_metrics,
        next_steps: processingResults.is_complete ? [
          '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          `Successfully processed all ${allConversions.length} conversions in database`,
          'System ready for complete multi-touch attribution analysis',
          'Use query-customer-journeys.js for comprehensive business intelligence'
        ] : [
          `Continue processing: ${processingResults.conversions_remaining} conversions remaining`,
          'Run the same command again to continue automatically',
          'Each run will process remaining conversions until timeout',
          'No manual tracking needed - system finds remaining work automatically',
          `Database completion: ${(((allConversions.length - processingResults.conversions_remaining) / allConversions.length) * 100).toFixed(1)}%`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Stateless journey processing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Stateless journey processing failed', 
        message: error.message 
      })
    };
  }
};

// STEP 1: Load ALL conversions (no date limits - scan entire database)
async function loadAllConversionsForJourneyBuilding(redis, maxProcessingTime) {
  console.log('🔍 Scanning ALL conversions in database for journey building...');
  
  const startTime = Date.now();
  const conversions = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    // Check timeout
    if (Date.now() - startTime > maxProcessingTime - 5000) {
      console.log('⏰ Time limit during conversion scan, stopping');
      break;
    }
    
    try {
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
        if (Date.now() - startTime > maxProcessingTime - 3000) break;
        
        const batch = keys.slice(i, i + batchSize);
        const batchConversions = await loadConversionBatch(redis, batch);
        conversions.push(...batchConversions);
      }
      
      if (conversions.length % 500 === 0 && conversions.length > 0) {
        console.log(`📊 Conversion scan progress: ${conversions.length} conversions loaded`);
      }
      
    } catch (scanError) {
      console.log(`⚠️ Conversion scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  // Sort by timestamp (most recent first for better processing efficiency)
  conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`✅ Conversion scan complete: ${conversions.length} total conversions found`);
  return conversions;
}

// Helper: Load a batch of conversions
async function loadConversionBatch(redis, conversionKeys) {
  const conversions = [];
  
  const batchPromises = conversionKeys.map(async (key) => {
    try {
      const conversionData = await redis(`get/${key}`);
      if (conversionData?.result) {
        const conversion = JSON.parse(decodeURIComponent(conversionData.result));
        
        // Validate conversion has required fields
        if (conversion.timestamp && conversion.email && conversion.order_id) {
          return {
            email: conversion.email,
            timestamp: conversion.timestamp,
            order_id: conversion.order_id,
            order_total: conversion.order_total || 0,
            
            // Attribution signals for journey reconstruction
            session_id: conversion.session_id,
            device_signature: conversion.device_signature || conversion.dsig,
            screen_value: conversion.screen_value || conversion.SVV || conversion.SVVV,
            gpu_signature: conversion.gpu_signature || conversion.gsig,
            
            // IP addresses for multi-IP attribution
            primary_ip: conversion.primary_ip,
            conversion_ip: conversion.conversion_ip,
            pageview_ip: conversion.pageview_ip,
            ip_addresses: [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean),
            
            // Current attribution (for comparison)
            current_attribution_found: conversion.attribution_found,
            current_attribution_method: conversion.attribution_method,
            current_source: conversion.source,
            current_landing_page: conversion.landing_page,
            
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
  
  return conversions;
}

// STEP 2: Filter out conversions that already have journeys (stateless check)
async function filterConversionsNeedingJourneys(redis, allConversions, maxProcessingTime) {
  console.log(`🔍 Checking ${allConversions.length} conversions for existing journeys...`);
  
  const startTime = Date.now();
  const conversionsNeedingJourneys = [];
  
  // Process conversions in batches to check for existing journeys
  const batchSize = 50;
  for (let i = 0; i < allConversions.length; i += batchSize) {
    if (Date.now() - startTime > maxProcessingTime - 2000) {
      console.log('⏰ Time limit during journey check, stopping');
      break;
    }
    
    const batch = allConversions.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (conversion) => {
      try {
        // Check for existing journey using order_id pattern
        // Pattern: customer_journey:journey_{order_id}_{timestamp}
        const searchPattern = `customer_journey:journey_${conversion.order_id}_*`;
        const existingJourneys = await redis(`keys/${searchPattern}`);
        
        // If no existing journey found, include in processing list
        if (!existingJourneys.result || existingJourneys.result.length === 0) {
          return conversion;
        }
        
        return null; // Journey already exists
      } catch (error) {
        // Include on error for safety (better to reprocess than miss)
        console.warn(`⚠️ Error checking journey for order ${conversion.order_id}: ${error.message}`);
        return conversion;
      }
    });
    
    const batchResults = await Promise.all(batchPromises);
    const validResults = batchResults.filter(result => result !== null);
    conversionsNeedingJourneys.push(...validResults);
    
    if ((i + batchSize) % 200 === 0) {
      console.log(`📊 Journey check progress: ${i + batchSize}/${allConversions.length} checked, ${conversionsNeedingJourneys.length} need journeys`);
    }
  }
  
  console.log(`✅ Journey check complete: ${conversionsNeedingJourneys.length} conversions need journey processing`);
  return conversionsNeedingJourneys;
}

// STEP 3: Process conversions in batches until timeout (stateless)
async function processConversionsUntilTimeout(redis, conversionsToProcess, queryEnhancedAttribution, journeyWindowHours, maxProcessingTime) {
  const startTime = Date.now();
  let journeysCreated = 0;
  let conversionsProcessed = 0;
  
  const qualityMetrics = {
    multi_touchpoint_journeys: 0,
    session_linked_journeys: 0,
    device_linked_journeys: 0,
    cross_session_journeys: 0,
    total_journey_span_hours: 0
  };
  
  console.log(`🚀 Processing ${conversionsToProcess.length} conversions until timeout...`);
  
  const batchSize = 20; // Journey batch size
  for (let i = 0; i < conversionsToProcess.length; i += batchSize) {
    // Check timeout before each batch
    const timeRemaining = maxProcessingTime - (Date.now() - startTime);
    if (timeRemaining < 8000) { // Need 8 seconds minimum for a batch
      console.log(`⏰ Time limit reached after processing ${conversionsProcessed} conversions`);
      break;
    }
    
    const batch = conversionsToProcess.slice(i, i + batchSize);
    console.log(`🔗 Processing conversion batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${conversionsToProcess.length}`);
    
    // Process this batch
    const batchResults = await processSingleBatch(redis, batch, queryEnhancedAttribution, journeyWindowHours);
    
    // Store journey records
    const storageResults = await storeCustomerJourneys(redis, batchResults.journeys);
    
    // Update metrics
    journeysCreated += batchResults.journeys.length;
    conversionsProcessed += batch.length;
    
    // Accumulate quality metrics
    qualityMetrics.multi_touchpoint_journeys += batchResults.quality_metrics.multi_touchpoint_journeys;
    qualityMetrics.session_linked_journeys += batchResults.quality_metrics.session_linked_journeys;
    qualityMetrics.device_linked_journeys += batchResults.quality_metrics.device_linked_journeys;
    qualityMetrics.cross_session_journeys += batchResults.quality_metrics.cross_session_journeys;
    qualityMetrics.total_journey_span_hours += batchResults.quality_metrics.total_journey_span_hours;
    
    console.log(`✅ Batch complete: ${batchResults.journeys.length} journeys created (${conversionsProcessed}/${conversionsToProcess.length} total)`);
  }
  
  const remainingConversions = conversionsToProcess.length - conversionsProcessed;
  
  // Calculate final quality metrics
  qualityMetrics.avg_journey_span_hours = qualityMetrics.total_journey_span_hours / (journeysCreated || 1);
  
  return {
    journeys_created_this_run: journeysCreated,
    conversions_processed_this_run: conversionsProcessed,
    conversions_remaining: remainingConversions,
    is_complete: remainingConversions === 0,
    processing_time_ms: Date.now() - startTime,
    quality_metrics: qualityMetrics
  };
}

// Process a single batch of conversions
async function processSingleBatch(redis, conversions, queryEnhancedAttribution, journeyWindowHours) {
  const journeys = [];
  const qualityMetrics = {
    multi_touchpoint_journeys: 0,
    session_linked_journeys: 0,
    device_linked_journeys: 0,
    cross_session_journeys: 0,
    total_journey_span_hours: 0
  };
  
  // Process conversions sequentially for timeout safety
  for (let i = 0; i < conversions.length; i++) {
    const conversion = conversions[i];
    
    try {
      // Query for all pageviews related to this conversion using enhanced attribution
      const journeyPageviews = await queryEnhancedAttribution(redis, {
        conversion_timestamp: conversion.timestamp,
        ips_to_check: conversion.ip_addresses,
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature,
        window_hours: journeyWindowHours
      });
      
      if (journeyPageviews && journeyPageviews.matches_found.length > 0) {
        // Build complete customer journey
        const journey = buildJourneyFromPageviews(conversion, journeyPageviews.matches_found);
        
        // Track quality metrics
        updateQualityMetrics(journey, qualityMetrics);
        journeys.push(journey);
      } else {
        // Even if no pageviews found, create journey record with conversion only
        const conversionOnlyJourney = createConversionOnlyJourney(conversion);
        updateQualityMetrics(conversionOnlyJourney, qualityMetrics);
        journeys.push(conversionOnlyJourney);
      }
      
      if ((i + 1) % 5 === 0) {
        console.log(`🔗 Journey building progress: ${i + 1}/${conversions.length} conversions processed`);
      }
      
    } catch (journeyError) {
      console.warn(`⚠️ Error building journey for conversion ${conversion.order_id}:`, journeyError.message);
      const fallbackJourney = createConversionOnlyJourney(conversion);
      updateQualityMetrics(fallbackJourney, qualityMetrics);
      journeys.push(fallbackJourney);
    }
  }
  
  return {
    journeys,
    quality_metrics: qualityMetrics
  };
}

// Build journey from pageviews and conversion (IDENTICAL to original)
function buildJourneyFromPageviews(conversion, pageviews) {
  // Sort pageviews by timestamp (earliest first for journey timeline)
  const sortedPageviews = pageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Create touchpoints from pageviews
  const touchpoints = sortedPageviews.map((pageview, index) => ({
    touchpoint_id: `${conversion.order_id}_${index + 1}`,
    timestamp: pageview.timestamp,
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
    
    // Session and device data for linking analysis
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
    confidence: 1000, // Highest confidence for actual conversion
    touchpoint_position: touchpoints.length + 1,
    is_conversion: true,
    is_last_touchpoint: true
  });
  
  // Calculate journey metrics
  const journeyStart = new Date(touchpoints[0].timestamp);
  const journeyEnd = new Date(conversion.timestamp);
  const journeySpanHours = (journeyEnd - journeyStart) / (1000 * 60 * 60);
  
  // Analyze journey characteristics
  const uniqueSessions = new Set(touchpoints.map(t => t.session_id).filter(Boolean)).size;
  const uniqueDeviceFingerprints = new Set(touchpoints.map(t => t.canvas_fingerprint).filter(Boolean)).size;
  const uniqueSources = new Set(touchpoints.map(t => t.source).filter(Boolean));
  
  return {
    journey_id: `journey_${conversion.order_id}_${Date.now()}`,
    customer_email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    conversion_order_id: conversion.order_id,
    conversion_value: conversion.order_total,
    
    // Journey timeline
    journey_start: touchpoints[0].timestamp,
    journey_end: conversion.timestamp,
    journey_span_hours: journeySpanHours,
    total_touchpoints: touchpoints.length,
    
    // Journey characteristics
    unique_sessions: uniqueSessions,
    unique_device_fingerprints: uniqueDeviceFingerprints,
    unique_sources: Array.from(uniqueSources),
    cross_session_journey: uniqueSessions > 1,
    cross_device_journey: uniqueDeviceFingerprints > 1,
    
    // Attribution analysis
    first_click_source: touchpoints[0].source,
    last_click_source: touchpoints[touchpoints.length - 2]?.source || touchpoints[0].source, // Last pageview before conversion
    attribution_confidence_avg: touchpoints.reduce((sum, t) => sum + (t.confidence || 0), 0) / touchpoints.length,
    
    // Complete touchpoint timeline
    touchpoints: touchpoints,
    
    // Journey metadata
    created_at: new Date().toISOString(),
    reconstruction_method: 'enhanced_multi_signal_attribution_stateless'
  };
}

// Create conversion-only journey (IDENTICAL to original)
function createConversionOnlyJourney(conversion) {
  return {
    journey_id: `journey_${conversion.order_id}_conversion_only`,
    customer_email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    conversion_order_id: conversion.order_id,
    conversion_value: conversion.order_total,
    
    journey_start: conversion.timestamp,
    journey_end: conversion.timestamp,
    journey_span_hours: 0,
    total_touchpoints: 1,
    
    unique_sessions: 0,
    unique_device_fingerprints: 0,
    unique_sources: [conversion.current_source || 'unknown'],
    cross_session_journey: false,
    cross_device_journey: false,
    
    first_click_source: conversion.current_source || 'unknown',
    last_click_source: conversion.current_source || 'unknown',
    attribution_confidence_avg: 0,
    
    touchpoints: [{
      touchpoint_id: `${conversion.order_id}_conversion_only`,
      timestamp: conversion.timestamp,
      type: 'conversion',
      order_id: conversion.order_id,
      order_total: conversion.order_total,
      email: conversion.email,
      source: conversion.current_source || 'unknown',
      attribution_method: 'conversion_only',
      confidence: 100,
      touchpoint_position: 1,
      is_conversion: true,
      is_first_touchpoint: true,
      is_last_touchpoint: true
    }],
    
    created_at: new Date().toISOString(),
    reconstruction_method: 'conversion_only_fallback_stateless'
  };
}

// Update quality metrics (IDENTICAL to original)
function updateQualityMetrics(journey, metrics) {
  if (journey.total_touchpoints > 1) {
    metrics.multi_touchpoint_journeys++;
  }
  
  if (journey.unique_sessions > 0) {
    metrics.session_linked_journeys++;
  }
  
  if (journey.unique_device_fingerprints > 0) {
    metrics.device_linked_journeys++;
  }
  
  if (journey.cross_session_journey) {
    metrics.cross_session_journeys++;
  }
  
  metrics.total_journey_span_hours += journey.journey_span_hours;
}

// Store customer journeys (IDENTICAL to original)
async function storeCustomerJourneys(redis, journeys) {
  console.log(`💾 Storing ${journeys.length} customer journeys...`);
  
  let journeysStored = 0;
  
  // Store individual journey records using IDENTICAL key pattern
  for (const journey of journeys) {
    try {
      const journeyKey = `customer_journey:${journey.journey_id}`;
      await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
      journeysStored++;
      
    } catch (storageError) {
      console.warn(`⚠️ Error storing journey ${journey.journey_id}:`, storageError.message);
    }
  }
  
  console.log(`✅ Journey storage complete: ${journeysStored} journeys stored`);
  
  return {
    journeys_stored: journeysStored
  };
}

// Update journey analytics (same key as original)
async function updateJourneyAnalytics(redis, totalConversionsProcessed) {
  try {
    console.log(`📊 Updating journey analytics for complete dataset...`);
    
    // Load all journey records to calculate final analytics
    const allJourneys = await loadAllJourneyRecords(redis);
    
    if (allJourneys.length > 0) {
      const analyticsKey = 'customer_journey_analytics';
      const analyticsData = {
        total_journeys: allJourneys.length,
        journeys_with_multiple_touchpoints: allJourneys.filter(j => j.total_touchpoints > 1).length,
        cross_session_journeys: allJourneys.filter(j => j.cross_session_journey).length,
        cross_device_journeys: allJourneys.filter(j => j.cross_device_journey).length,
        avg_touchpoints: allJourneys.reduce((sum, j) => sum + j.total_touchpoints, 0) / allJourneys.length,
        avg_journey_span_hours: allJourneys.reduce((sum, j) => sum + j.journey_span_hours, 0) / allJourneys.length,
        total_conversion_value: allJourneys.reduce((sum, j) => sum + j.conversion_value, 0),
        created_at: new Date().toISOString(),
        build_completed_at: new Date().toISOString(),
        processing_method: 'stateless_complete_database_scan'
      };
      
      await redis(`setex/${analyticsKey}/2592000/${encodeURIComponent(JSON.stringify(analyticsData))}`);
      console.log(`✅ Journey analytics updated for ${allJourneys.length} total journeys`);
    }
    
  } catch (analyticsError) {
    console.warn('⚠️ Error updating journey analytics:', analyticsError.message);
  }
}

// Load all journey records for analytics
async function loadAllJourneyRecords(redis) {
  const journeys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Load journey data
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`);
            if (journeyData?.result) {
              return JSON.parse(decodeURIComponent(journeyData.result));
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(journey => journey !== null);
        journeys.push(...validJourneys);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Error loading journey records: ${scanError.message}`);
  }
  
  return journeys;
}

// Create enhanced attribution querier (IDENTICAL to original)
function createEnhancedAttributionQuerier() {
  return async function queryEnhancedAttribution(redis, params) {
    try {
      // Validate API key is available
      const apiKey = process.env.OJOY_API_KEY;
      if (!apiKey) {
        console.error('❌ OJOY_API_KEY environment variable not set');
        return { matches_found: [] };
      }
      
      // Use the existing query-pageviews-enhanced.js logic
      const queryUrl = `${process.env.NETLIFY_URL || 'https://trackingojoy.netlify.app'}/.netlify/functions/query-pageviews-enhanced`;
      
      const response = await fetch(queryUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': apiKey
        },
        body: JSON.stringify(params)
      });
      
      if (response.ok) {
        return await response.json();
      } else {
        console.warn('⚠️ Enhanced attribution query failed:', response.status);
        return { matches_found: [] };
      }
      
    } catch (queryError) {
      console.warn('⚠️ Enhanced attribution query error:', queryError.message);
      return { matches_found: [] };
    }
  };
}

// Initialize Redis helper (IDENTICAL to original)
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
