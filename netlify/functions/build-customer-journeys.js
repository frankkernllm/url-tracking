// netlify/functions/build-customer-journeys.js
// Customer Journey Reconstruction Engine
// Builds complete customer journeys from conversions using enhanced multi-signal attribution

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
    console.log('🚀 CUSTOMER JOURNEY RECONSTRUCTION: Starting journey building...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    const queryEnhancedAttribution = createEnhancedAttributionQuerier();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      date_range_days = 7,        // How many days of conversions to process
      journey_window_hours = 168, // 7-day journey lookback window
      batch_size = 50,           // Process conversions in batches
      rebuild_existing = false,   // Whether to rebuild existing journeys
      skip_existing = false       // Whether to skip already processed conversions
    } = body;
    
    console.log(`📊 Journey Parameters: ${date_range_days} days of conversions, ${journey_window_hours}h lookback window`);
    if (skip_existing) {
      console.log(`🔄 Resume mode: Will skip existing journeys and process only new conversions`);
    }
    
    // Step 1: Load conversions for journey reconstruction
    const conversions = await loadConversionsForJourneyBuilding(redis, date_range_days);
    console.log(`💰 Found ${conversions.length} conversions for journey reconstruction`);
    
    if (conversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No conversions found for journey building'
        })
      };
    }
    
    // Step 1.5: Filter out existing journeys if skip_existing is enabled
    let conversionsToProcess = conversions;
    let existingJourneys = [];
    
    if (skip_existing) {
      console.log(`🔍 Checking for existing journeys to skip...`);
      const existingJourneyData = await findExistingJourneys(redis);
      existingJourneys = existingJourneyData.existing_journeys;
      
      const existingOrderIds = new Set(existingJourneys.map(j => j.conversion_order_id));
      conversionsToProcess = conversions.filter(conv => !existingOrderIds.has(conv.order_id));
      
      console.log(`📊 Resume Status: ${existingJourneys.length} existing journeys found, ${conversionsToProcess.length} new conversions to process`);
      
      if (conversionsToProcess.length === 0) {
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({
            success: true,
            message: 'All conversions already have journeys - nothing to process',
            existing_journeys: existingJourneys.length,
            new_conversions: 0,
            resume_summary: {
              total_conversions_found: conversions.length,
              existing_journeys_skipped: existingJourneys.length,
              new_conversions_processed: 0
            }
          })
        };
      }
    }
    
    // Step 2: Build customer journeys using enhanced attribution
    const journeyResults = await buildCustomerJourneysFromConversions(
      redis, 
      conversionsToProcess,  // Use filtered conversions 
      queryEnhancedAttribution, 
      journey_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    // Step 3: Store journey records and build analytics
    const storageResults = await storeCustomerJourneys(redis, journeyResults.journeys);
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Customer journey reconstruction complete in ${totalTime}ms`);
    
    // Build response with resume information if applicable
    const response = {
      success: true,
      journey_reconstruction_summary: {
        conversions_processed: conversionsToProcess.length,
        journeys_created: journeyResults.journeys.length,
        total_touchpoints: journeyResults.total_touchpoints,
        avg_touchpoints_per_journey: journeyResults.total_touchpoints / journeyResults.journeys.length,
        journey_success_rate: ((journeyResults.journeys.length / conversionsToProcess.length) * 100).toFixed(2),
        processing_time_ms: totalTime
      },
      journey_quality_metrics: {
        journeys_with_multiple_touchpoints: journeyResults.quality_metrics.multi_touchpoint_journeys,
        journeys_with_session_linking: journeyResults.quality_metrics.session_linked_journeys,
        journeys_with_device_linking: journeyResults.quality_metrics.device_linked_journeys,
        journeys_with_cross_session_activity: journeyResults.quality_metrics.cross_session_journeys,
        average_journey_span_hours: journeyResults.quality_metrics.avg_journey_span_hours
      },
      attribution_analysis: {
        attribution_method_distribution: journeyResults.attribution_methods,
        confidence_score_distribution: journeyResults.confidence_distribution,
        multi_signal_success_rate: journeyResults.multi_signal_success_rate
      },
      storage_results: storageResults,
      next_steps: [
        'Use journey data for multi-touch attribution analysis',
        'Analyze customer behavior patterns across touchpoints',
        'Compare first-click vs last-click attribution models'
      ]
    };
    
    // Add resume information if skip_existing was used
    if (skip_existing) {
      response.resume_summary = {
        total_conversions_found: conversions.length,
        existing_journeys_skipped: existingJourneys.length,
        new_conversions_processed: conversionsToProcess.length,
        resume_mode_enabled: true,
        completion_status: conversionsToProcess.length === 0 ? 'complete' : 'partial'
      };
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(response)
    };
    
  } catch (error) {
    console.error('❌ Customer journey reconstruction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Customer journey reconstruction failed', 
        message: error.message 
      })
    };
  }
};

// Load conversions for journey building
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

// Find existing journeys to avoid reprocessing (for resume functionality)
async function findExistingJourneys(redis) {
  console.log(`🔍 Scanning for existing customer journeys...`);
  
  const existingJourneys = [];
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
      
      // Load journey data to get conversion order IDs
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              if (journey.conversion_order_id) {
                return {
                  journey_id: journey.journey_id,
                  conversion_order_id: journey.conversion_order_id,
                  customer_email: journey.customer_email,
                  conversion_timestamp: journey.conversion_timestamp
                };
              }
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        existingJourneys.push(...validResults);
      }
      
      if (existingJourneys.length % 100 === 0 && existingJourneys.length > 0) {
        console.log(`🔍 Existing journey scan progress: ${existingJourneys.length} journeys found`);
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Error scanning existing journeys: ${scanError.message}`);
  }
  
  console.log(`✅ Existing journey scan complete: ${existingJourneys.length} existing journeys found`);
  
  return {
    existing_journeys: existingJourneys,
    scan_iterations: iterations
  };
}

// Build customer journeys from conversions using enhanced attribution
async function buildCustomerJourneysFromConversions(redis, conversions, queryEnhancedAttribution, journeyWindowHours, batchSize, maxTime) {
  const buildStartTime = Date.now();
  console.log(`🔗 Building customer journeys for ${conversions.length} conversions (batch size: ${batchSize})...`);
  
  const journeys = [];
  const qualityMetrics = {
    multi_touchpoint_journeys: 0,
    session_linked_journeys: 0,
    device_linked_journeys: 0,
    cross_session_journeys: 0,
    total_journey_span_hours: 0
  };
  
  const attributionMethods = {};
  const confidenceDistribution = {};
  let multiSignalSuccesses = 0;
  let totalTouchpoints = 0;
  
  // Process conversions in batches
  for (let i = 0; i < conversions.length; i += batchSize) {
    if (Date.now() - buildStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit reached during journey building`);
      break;
    }
    
    const batch = conversions.slice(i, i + batchSize);
    console.log(`🔗 Processing journey batch ${Math.floor(i/batchSize) + 1}: ${batch.length} conversions (${i + batch.length}/${conversions.length} total)`);
    
    const batchPromises = batch.map(async (conversion) => {
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
          
          // Track attribution methods and confidence
          journeyPageviews.matches_found.forEach(match => {
            const method = match.attribution_method || 'unknown';
            attributionMethods[method] = (attributionMethods[method] || 0) + 1;
            
            const confidenceRange = getConfidenceRange(match.confidence);
            confidenceDistribution[confidenceRange] = (confidenceDistribution[confidenceRange] || 0) + 1;
            
            if (match.confidence > 250) multiSignalSuccesses++;
          });
          
          totalTouchpoints += journey.touchpoints.length;
          return journey;
        }
        
        // Even if no pageviews found, create journey record with conversion only
        return createConversionOnlyJourney(conversion);
        
      } catch (journeyError) {
        console.warn(`⚠️ Error building journey for conversion ${conversion.order_id}:`, journeyError.message);
        return createConversionOnlyJourney(conversion);
      }
    });
    
    const batchJourneys = await Promise.all(batchPromises);
    journeys.push(...batchJourneys.filter(journey => journey !== null));
    
    if ((i + batchSize) % (batchSize * 2) === 0) {
      console.log(`🔗 Journey building progress: ${journeys.length} journeys from ${i + batchSize}/${conversions.length} conversions`);
    }
  }
  
  // Calculate final metrics
  qualityMetrics.avg_journey_span_hours = qualityMetrics.total_journey_span_hours / journeys.length;
  const multiSignalSuccessRate = ((multiSignalSuccesses / totalTouchpoints) * 100).toFixed(2);
  
  console.log(`✅ Customer journey building complete: ${journeys.length} journeys created`);
  
  return {
    journeys,
    total_touchpoints: totalTouchpoints,
    quality_metrics: qualityMetrics,
    attribution_methods: attributionMethods,
    confidence_distribution: confidenceDistribution,
    multi_signal_success_rate: multiSignalSuccessRate
  };
}

// Build journey from pageviews and conversion
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

// Create journey record for conversions without found pageviews
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

// Update quality metrics during journey building
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

// Get confidence range for distribution analysis
function getConfidenceRange(confidence) {
  if (confidence >= 290) return 'very_high_290+';
  if (confidence >= 250) return 'high_250-289';
  if (confidence >= 200) return 'medium_200-249';
  if (confidence >= 150) return 'low_150-199';
  return 'very_low_below_150';
}

// Store customer journeys in Redis
async function storeCustomerJourneys(redis, journeys) {
  console.log(`💾 Storing ${journeys.length} customer journeys...`);
  
  let journeysStored = 0;
  let indexesCreated = 0;
  
  // Store individual journey records
  for (const journey of journeys) {
    try {
      const journeyKey = `customer_journey:${journey.journey_id}`;
      await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
      journeysStored++;
      
    } catch (storageError) {
      console.warn(`⚠️ Error storing journey ${journey.journey_id}:`, storageError.message);
    }
  }
  
  // Create journey analytics index
  try {
    const analyticsKey = 'customer_journey_analytics';
    const analyticsData = {
      total_journeys: journeys.length,
      journeys_with_multiple_touchpoints: journeys.filter(j => j.total_touchpoints > 1).length,
      cross_session_journeys: journeys.filter(j => j.cross_session_journey).length,
      cross_device_journeys: journeys.filter(j => j.cross_device_journey).length,
      avg_touchpoints: journeys.reduce((sum, j) => sum + j.total_touchpoints, 0) / journeys.length,
      avg_journey_span_hours: journeys.reduce((sum, j) => sum + j.journey_span_hours, 0) / journeys.length,
      total_conversion_value: journeys.reduce((sum, j) => sum + j.conversion_value, 0),
      created_at: new Date().toISOString()
    };
    
    await redis(`setex/${analyticsKey}/2592000/${encodeURIComponent(JSON.stringify(analyticsData))}`);
    indexesCreated++;
    
  } catch (analyticsError) {
    console.warn('⚠️ Error creating journey analytics index:', analyticsError.message);
  }
  
  console.log(`✅ Journey storage complete: ${journeysStored} journeys stored, ${indexesCreated} indexes created`);
  
  return {
    journeys_stored: journeysStored,
    indexes_created: indexesCreated,
    storage_keys_created: journeysStored + indexesCreated
  };
}

// Create enhanced attribution querier using existing query-pageviews-enhanced.js
function createEnhancedAttributionQuerier() {
  return async function queryEnhancedAttribution(redis, params) {
    try {
      // Use the existing query-pageviews-enhanced.js logic
      // This calls your enhanced attribution function with required API key
      
      const queryUrl = `${process.env.NETLIFY_URL || 'https://trackingojoy.netlify.app'}/.netlify/functions/query-pageviews-enhanced`;
      
      const response = await fetch(queryUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': process.env.OJOY_API_KEY  // Use environment variable (required)
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
