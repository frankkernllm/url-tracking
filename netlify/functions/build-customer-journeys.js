// netlify/functions/build-customer-journeys.js
// RESUME-CAPABLE Customer Journey Reconstruction Engine
// SAFE: Does not modify existing Redis key patterns, maintains identical data structure

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
    console.log('🔄 RESUME-CAPABLE CUSTOMER JOURNEY RECONSTRUCTION: Starting smart resume...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max (5 second buffer)
    
    // Validate required environment variables
    if (!process.env.OJOY_API_KEY) {
      return {
        statusCode: 500,
        headers,
        body: JSON.stringify({
          error: 'Configuration error',
          message: 'OJOY_API_KEY environment variable not set',
          required_env_vars: ['OJOY_API_KEY', 'UPSTASH_REDIS_REST_URL', 'UPSTASH_REDIS_REST_TOKEN']
        })
      };
    }
    
    const redis = initializeRedis();
    const queryEnhancedAttribution = createEnhancedAttributionQuerier();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      date_range_days = 7,        // How many days of conversions to process
      journey_window_hours = 168, // 7-day journey lookback window
      batch_size = 20,           // REDUCED: Safe batch size for timeout protection
      force_rebuild = false,     // Whether to rebuild ALL journeys (ignore existing)
      reset_progress = false     // Whether to reset progress tracking and start fresh
    } = body;
    
    console.log(`📊 Resume Parameters: ${date_range_days} days, ${journey_window_hours}h window, batch size: ${batch_size}`);
    if (force_rebuild) {
      console.log(`🔄 FORCE REBUILD: Will rebuild all journeys regardless of existing records`);
    }
    if (reset_progress) {
      console.log(`🔄 RESET PROGRESS: Will clear progress tracking and start from batch 1`);
    }
    
    // Step 1: Load progress tracking to resume from last position
    const progressKey = 'customer_journey_build_progress';
    let buildProgress;
    
    if (reset_progress) {
      // Clear progress and start fresh
      buildProgress = {
        journeys_completed: 0,
        last_batch_completed: 0,
        started_at: new Date().toISOString(),
        total_conversions_to_process: 0
      };
      console.log(`📋 Progress Reset: Starting fresh from batch 1`);
    } else {
      buildProgress = await loadBuildProgress(redis, progressKey);
      console.log(`📋 Previous Progress: ${buildProgress.journeys_completed} journeys built, last batch: ${buildProgress.last_batch_completed}`);
    }
    
    // Step 2: Load conversions for journey reconstruction
    const allConversions = await loadConversionsForJourneyBuilding(redis, date_range_days);
    console.log(`💰 Found ${allConversions.length} total conversions in ${date_range_days}-day range`);
    
    if (allConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No conversions found for journey building',
          resume_info: {
            total_conversions: 0,
            journeys_completed: buildProgress.journeys_completed
          }
        })
      };
    }
    
    // Step 3: Identify which conversions already have journeys (SAFE: reads existing keys)
    let conversionsToProcess;
    if (force_rebuild) {
      conversionsToProcess = allConversions;
      console.log(`🔄 FORCE REBUILD: Processing all ${allConversions.length} conversions (ignoring existing journeys)`);
    } else {
      const existingJourneys = await findExistingJourneyOrderIds(redis);
      console.log(`🔍 Found ${existingJourneys.length} existing journey records`);
      
      // DEBUG: Check data types and sample values
      const sampleConversions = allConversions.slice(0, 3).map(c => ({ order_id: c.order_id, type: typeof c.order_id }));
      const sampleExistingIds = existingJourneys.slice(0, 5).map(id => ({ order_id: id, type: typeof id }));
      const sampleExistingIdsEnd = existingJourneys.slice(-5).map(id => ({ order_id: id, type: typeof id }));
      console.log(`🔍 DEBUG Sample conversions:`, sampleConversions);
      console.log(`🔍 DEBUG Sample existing journey order IDs (first 5):`, sampleExistingIds);
      console.log(`🔍 DEBUG Sample existing journey order IDs (last 5):`, sampleExistingIdsEnd);
      
      // Normalize data types for comparison (convert all to strings)
      const existingOrderIds = new Set(existingJourneys.map(id => String(id)));
      conversionsToProcess = allConversions.filter(conv => !existingOrderIds.has(String(conv.order_id)));
      
      console.log(`📊 Resume Status: ${existingJourneys.length} existing, ${conversionsToProcess.length} new conversions to process`);
      
      // DEBUG: Show some conversions that would be processed
      if (conversionsToProcess.length > 0) {
        const sampleToProcess = conversionsToProcess.slice(0, 5).map(c => c.order_id);
        console.log(`🔍 DEBUG Sample conversions to process:`, sampleToProcess);
      } else {
        console.log(`🔍 DEBUG: No conversions to process`);
        // Show range of existing order IDs to understand the coverage
        const existingOrderIdsArray = Array.from(existingOrderIds).map(id => parseInt(id)).sort((a, b) => a - b);
        const minExisting = existingOrderIdsArray[0];
        const maxExisting = existingOrderIdsArray[existingOrderIdsArray.length - 1];
        console.log(`🔍 DEBUG Existing journey order ID range: ${minExisting} to ${maxExisting} (${existingOrderIdsArray.length} total)`);
        
        const conversionOrderIds = allConversions.map(c => parseInt(c.order_id)).sort((a, b) => a - b);
        const minConversion = conversionOrderIds[0];
        const maxConversion = conversionOrderIds[conversionOrderIds.length - 1];
        console.log(`🔍 DEBUG Conversion order ID range: ${minConversion} to ${maxConversion} (${conversionOrderIds.length} total)`);
      }
    }
    
    if (conversionsToProcess.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'All conversions already have journeys - build complete!',
          completion_summary: {
            total_conversions_found: allConversions.length,
            existing_journeys_found: allConversions.length,
            new_conversions_processed: 0,
            build_status: 'complete'
          }
        })
      };
    }
    
    // Step 4: Calculate batch range for this run (RESUME LOGIC)
    let startIndex, endIndex, thisBatchConversions;
    
    if (force_rebuild || reset_progress) {
      // For force rebuild or reset, ignore previous progress and start from beginning
      startIndex = 0;
      endIndex = Math.min(batch_size, conversionsToProcess.length);
      thisBatchConversions = conversionsToProcess.slice(startIndex, endIndex);
      console.log(`🎯 This Run (${force_rebuild ? 'FORCE REBUILD' : 'RESET'}): Processing conversions 1-${endIndex} of ${conversionsToProcess.length} (batch 1)`);
    } else {
      // Normal resume logic
      startIndex = buildProgress.last_batch_completed * batch_size;
      endIndex = Math.min(startIndex + batch_size, conversionsToProcess.length);
      thisBatchConversions = conversionsToProcess.slice(startIndex, endIndex);
      console.log(`🎯 This Run: Processing conversions ${startIndex + 1}-${endIndex} of ${conversionsToProcess.length} (batch ${buildProgress.last_batch_completed + 1})`);
    }
    
    if (thisBatchConversions.length === 0) {
      // All conversions processed, update final analytics
      await updateJourneyAnalytics(redis, conversionsToProcess.length);
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'Customer journey building COMPLETE!',
          completion_summary: {
            total_conversions_found: allConversions.length,
            total_conversions_processed: conversionsToProcess.length,
            final_analytics_updated: true,
            build_status: 'complete'
          }
        })
      };
    }
    
    // Step 5: Build customer journeys for this batch using IDENTICAL logic
    const journeyResults = await buildCustomerJourneysFromConversions(
      redis, 
      thisBatchConversions,
      queryEnhancedAttribution, 
      journey_window_hours,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Step 6: Store journey records using IDENTICAL structure (SAFE: same keys as original)
    const storageResults = await storeCustomerJourneys(redis, journeyResults.journeys);
    
    // Step 7: Update progress tracking for next run
    let updatedProgress;
    
    if (force_rebuild || reset_progress) {
      // For force rebuild or reset, start progress tracking fresh
      updatedProgress = {
        journeys_completed: journeyResults.journeys.length,
        last_batch_completed: 1,
        started_at: new Date().toISOString(),
        last_updated: new Date().toISOString(),
        conversions_processed_this_run: thisBatchConversions.length,
        total_conversions_to_process: conversionsToProcess.length,
        mode: force_rebuild ? 'force_rebuild' : 'reset_progress'
      };
    } else {
      // Normal progress update
      updatedProgress = {
        ...buildProgress,
        journeys_completed: buildProgress.journeys_completed + journeyResults.journeys.length,
        last_batch_completed: buildProgress.last_batch_completed + 1,
        last_updated: new Date().toISOString(),
        conversions_processed_this_run: thisBatchConversions.length,
        total_conversions_to_process: conversionsToProcess.length
      };
    }
    
    await storeBuildProgress(redis, progressKey, updatedProgress);
    
    const totalTime = Date.now() - startTime;
    const isComplete = endIndex >= conversionsToProcess.length;
    
    console.log(`✅ Batch ${buildProgress.last_batch_completed + 1} complete in ${totalTime}ms`);
    
    // Step 8: Update journey analytics if this is the final batch
    if (isComplete) {
      console.log(`🎉 FINAL BATCH: Updating journey analytics`);
      await updateJourneyAnalytics(redis, conversionsToProcess.length);
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        batch_complete: true,
        build_complete: isComplete,
        batch_summary: {
          batch_number: force_rebuild || reset_progress ? 1 : buildProgress.last_batch_completed + 1,
          conversions_processed_this_batch: thisBatchConversions.length,
          journeys_created_this_batch: journeyResults.journeys.length,
          processing_time_ms: totalTime,
          journey_success_rate: ((journeyResults.journeys.length / thisBatchConversions.length) * 100).toFixed(2)
        },
        progress_summary: {
          total_conversions_to_process: conversionsToProcess.length,
          conversions_processed_so_far: endIndex,
          journeys_completed_so_far: updatedProgress.journeys_completed,
          progress_percentage: ((endIndex / conversionsToProcess.length) * 100).toFixed(1),
          estimated_batches_remaining: Math.ceil((conversionsToProcess.length - endIndex) / batch_size)
        },
        journey_quality_metrics: {
          journeys_with_multiple_touchpoints: journeyResults.quality_metrics.multi_touchpoint_journeys,
          journeys_with_session_linking: journeyResults.quality_metrics.session_linked_journeys,
          average_journey_span_hours: journeyResults.quality_metrics.avg_journey_span_hours
        },
        next_steps: isComplete ? [
          '🎉 Customer journey building COMPLETE!',
          'All conversions now have journey records',
          'Use query-customer-journeys.js for business intelligence',
          'System ready for advanced attribution analysis'
        ] : [
          `Continue building: ${Math.ceil((conversionsToProcess.length - endIndex) / batch_size)} batches remaining`,
          'Run the same command again to process next batch',
          `Next batch will process conversions ${endIndex + 1}-${Math.min(endIndex + batch_size, conversionsToProcess.length)}`,
          `Progress saved automatically - safe to continue later`,
          `Use "force_rebuild": true to rebuild all journeys`,
          `Use "reset_progress": true to restart from batch 1`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Resume customer journey reconstruction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Resume customer journey reconstruction failed', 
        message: error.message 
      })
    };
  }
};

// SAFE: Load progress tracking (new key, doesn't interfere with existing system)
async function loadBuildProgress(redis, progressKey) {
  try {
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(decodeURIComponent(progressData.result));
      console.log(`📋 Resuming from saved progress: batch ${progress.last_batch_completed}`);
      return progress;
    }
  } catch (error) {
    console.log('📋 No existing progress found, starting fresh');
  }
  
  // Default fresh start
  return {
    journeys_completed: 0,
    last_batch_completed: 0,
    started_at: new Date().toISOString(),
    total_conversions_to_process: 0
  };
}

// SAFE: Store progress tracking (new key, doesn't interfere with existing system)
async function storeBuildProgress(redis, progressKey, progress) {
  await redis(`setex/${progressKey}/86400/${encodeURIComponent(JSON.stringify(progress))}`); // 24 hour TTL
  console.log(`💾 Progress saved: batch ${progress.last_batch_completed}, ${progress.journeys_completed} journeys completed`);
}

// SAFE: Find existing journey order IDs (reads existing keys without modification)
async function findExistingJourneyOrderIds(redis) {
  console.log(`🔍 Scanning for existing customer journey records...`);
  
  const existingOrderIds = new Set();
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
      
      // Load journey data to get conversion order IDs (SAFE: read-only)
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              if (journey.conversion_order_id) {
                return journey.conversion_order_id;
              }
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validOrderIds = batchResults.filter(orderId => orderId !== null);
        validOrderIds.forEach(orderId => existingOrderIds.add(orderId));
      }
      
      if (existingOrderIds.size % 100 === 0 && existingOrderIds.size > 0) {
        console.log(`🔍 Existing journey scan progress: ${existingOrderIds.size} order IDs found`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Error scanning existing journeys: ${scanError.message}`);
  }
  
  console.log(`✅ Existing journey scan complete: ${existingOrderIds.size} existing journeys found`);
  return Array.from(existingOrderIds);
}

// IDENTICAL: Load conversions (same logic as original)
async function loadConversionsForJourneyBuilding(redis, dateRangeDays) {
  console.log(`🔍 Loading conversions from last ${dateRangeDays} days...`);
  
  const conversions = [];
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - dateRangeDays);
  
  // Scan for recent conversion keys
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Process keys in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const conversionData = await redis(`get/${key}`);
            if (conversionData?.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // Filter by date range
              const conversionDate = new Date(conversion.timestamp);
              if (conversionDate >= cutoffDate && conversion.email) {
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
      }
      
    } catch (scanError) {
      console.log(`⚠️ Conversion scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  // Sort by timestamp (most recent first)
  conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`✅ Loaded ${conversions.length} conversions for journey building`);
  return conversions;
}

// IDENTICAL: Build customer journeys (same logic as original)
async function buildCustomerJourneysFromConversions(redis, conversions, queryEnhancedAttribution, journeyWindowHours, maxTime) {
  const buildStartTime = Date.now();
  console.log(`🔗 Building customer journeys for ${conversions.length} conversions...`);
  
  const journeys = [];
  const qualityMetrics = {
    multi_touchpoint_journeys: 0,
    session_linked_journeys: 0,
    device_linked_journeys: 0,
    cross_session_journeys: 0,
    total_journey_span_hours: 0
  };
  
  let totalTouchpoints = 0;
  
  // Process conversions sequentially for timeout safety
  for (let i = 0; i < conversions.length; i++) {
    if (Date.now() - buildStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit reached during journey building at conversion ${i + 1}/${conversions.length}`);
      break;
    }
    
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
        totalTouchpoints += journey.touchpoints.length;
        
        journeys.push(journey);
      } else {
        // Even if no pageviews found, create journey record with conversion only
        const conversionOnlyJourney = createConversionOnlyJourney(conversion);
        updateQualityMetrics(conversionOnlyJourney, qualityMetrics);
        totalTouchpoints += conversionOnlyJourney.touchpoints.length;
        
        journeys.push(conversionOnlyJourney);
      }
      
      if ((i + 1) % 5 === 0) {
        console.log(`🔗 Journey building progress: ${i + 1}/${conversions.length} conversions processed`);
      }
      
    } catch (journeyError) {
      console.warn(`⚠️ Error building journey for conversion ${conversion.order_id}:`, journeyError.message);
      const fallbackJourney = createConversionOnlyJourney(conversion);
      journeys.push(fallbackJourney);
    }
  }
  
  // Calculate final metrics
  qualityMetrics.avg_journey_span_hours = qualityMetrics.total_journey_span_hours / journeys.length;
  
  console.log(`✅ Customer journey building complete: ${journeys.length} journeys created`);
  
  return {
    journeys,
    total_touchpoints: totalTouchpoints,
    quality_metrics: qualityMetrics
  };
}

// IDENTICAL: Build journey from pageviews (same logic as original)
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
    reconstruction_method: 'enhanced_multi_signal_attribution'
  };
}

// IDENTICAL: Create conversion-only journey (same logic as original)
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
    reconstruction_method: 'conversion_only_fallback'
  };
}

// IDENTICAL: Update quality metrics (same logic as original)
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

// IDENTICAL: Store customer journeys (same Redis keys as original - SAFE)
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

// SAFE: Update journey analytics only when complete (same key as original)
async function updateJourneyAnalytics(redis, totalJourneysProcessed) {
  try {
    console.log(`📊 Updating journey analytics for ${totalJourneysProcessed} completed journeys...`);
    
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
        build_completed_at: new Date().toISOString()
      };
      
      await redis(`setex/${analyticsKey}/2592000/${encodeURIComponent(JSON.stringify(analyticsData))}`);
      console.log(`✅ Journey analytics updated for ${allJourneys.length} total journeys`);
    }
    
  } catch (analyticsError) {
    console.warn('⚠️ Error updating journey analytics:', analyticsError.message);
  }
}

// Helper: Load all journey records for analytics
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

// IDENTICAL: Create enhanced attribution querier (same as original)
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

// IDENTICAL: Initialize Redis helper (same as original)
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
