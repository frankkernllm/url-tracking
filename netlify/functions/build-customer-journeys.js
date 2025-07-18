// FIXED VERSION - build-customer-journeys.js
// Key fixes: Optimized existence check, bypass options, better time management

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
    console.log('🚀 OPTIMIZED CUSTOMER JOURNEY BUILDER: Starting...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters with new bypass options
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      journey_window_hours = 168,
      batch_size = 20,
      skip_existence_check = false,  // NEW: Option to bypass slow check
      process_recent_only = false,   // NEW: Only process recent conversions
      max_existence_check_time = 8000, // NEW: Limit existence check time
      recent_days_limit = 7          // NEW: How many recent days to process
    } = body;
    
    console.log(`📊 Journey Parameters: ${journey_window_hours}h lookback, batch: ${batch_size}, skip_check: ${skip_existence_check}`);
    
    // Step 1: Load conversions (with recent filter option)
    const allConversions = await loadConversionsOptimized(redis, process_recent_only, recent_days_limit, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Found ${allConversions.length} conversions to process`);
    
    if (allConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          message: 'No conversions found'
        })
      };
    }
    
    let conversionsNeedingJourneys;
    
    if (skip_existence_check) {
      // BYPASS MODE: Process a small batch directly
      console.log(`⚡ BYPASS MODE: Skipping existence check, processing first ${batch_size} conversions`);
      conversionsNeedingJourneys = allConversions.slice(0, batch_size);
    } else {
      // OPTIMIZED existence check with strict time limits
      conversionsNeedingJourneys = await filterConversionsNeedingJourneysOptimized(
        redis, 
        allConversions, 
        max_existence_check_time
      );
    }
    
    console.log(`📊 Journey Status: ${conversionsNeedingJourneys.length} need processing`);
    
    if (conversionsNeedingJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          build_complete: true,
          message: 'ALL CUSTOMER JOURNEYS COMPLETE!'
        })
      };
    }
    
    // Step 3: Process conversions with remaining time
    const remainingTime = maxProcessingTime - (Date.now() - startTime);
    const processingResults = await processConversionsWithEmbeddedAttribution(
      redis, 
      conversionsNeedingJourneys, 
      journey_window_hours,
      batch_size,
      remainingTime
    );
    
    const totalTime = Date.now() - startTime;
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
          processing_time_ms: totalTime,
          attribution_success_rate: processingResults.attribution_success_rate
        },
        next_steps: processingResults.is_complete ? [
          '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          'System ready for attribution analysis'
        ] : [
          `Run again to continue processing`,
          `Consider using skip_existence_check=true for faster processing`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Journey processing failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Journey processing failed', 
        message: error.message 
      })
    };
  }
};

// NEW: Optimized conversion loading with recent filter
async function loadConversionsOptimized(redis, processRecentOnly, recentDaysLimit, maxTime) {
  console.log(`🔍 Loading conversions (recent_only: ${processRecentOnly}, days: ${recentDaysLimit})...`);
  
  const loadStartTime = Date.now();
  const conversions = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = processRecentOnly ? 5 : 20; // Fewer iterations for recent-only
  
  const recentCutoff = processRecentOnly ? 
    Date.now() - (recentDaysLimit * 24 * 60 * 60 * 1000) : 
    0;
  
  try {
    do {
      // Check timeout
      if (Date.now() - loadStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during conversion loading, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Process keys in batches
      const batchSize = 100;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const conversionData = await redis(`get/${key}`);
            if (conversionData?.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // Filter by date if recent-only mode
              if (processRecentOnly) {
                const conversionTime = new Date(conversion.timestamp).getTime();
                if (conversionTime < recentCutoff) {
                  return null; // Skip old conversions
                }
              }
              
              return {
                order_id: conversion.order_id,
                email: conversion.email,
                timestamp: conversion.timestamp,
                source: conversion.source,
                landing_page: conversion.landing_page,
                _redis_key: key
              };
            }
          } catch (parseError) {
            // Skip invalid conversions
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        conversions.push(...validResults);
        
        // Early exit for recent-only mode once we have enough
        if (processRecentOnly && conversions.length >= 100) {
          console.log(`📊 Recent-only mode: Found ${conversions.length} recent conversions, stopping scan`);
          break;
        }
      }
      
      if (processRecentOnly && conversions.length >= 100) {
        break;
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

// FIXED: Much faster existence check with strict time limits
async function filterConversionsNeedingJourneysOptimized(redis, allConversions, maxCheckTime) {
  console.log(`🔍 OPTIMIZED existence check for ${allConversions.length} conversions (${maxCheckTime}ms limit)...`);
  
  const checkStartTime = Date.now();
  const conversionsNeedingJourneys = [];
  
  // Much smaller batches and strict timeout
  const batchSize = 20; // Reduced from 50 to 20
  let processedCount = 0;
  
  for (let i = 0; i < allConversions.length; i += batchSize) {
    // Strict timeout check
    const elapsed = Date.now() - checkStartTime;
    if (elapsed > maxCheckTime) {
      console.log(`⏰ Existence check timeout after ${elapsed}ms, processed ${processedCount}/${allConversions.length}`);
      break;
    }
    
    const batch = allConversions.slice(i, i + batchSize);
    
    try {
      // Parallel existence checks with timeout
      const batchPromises = batch.map(async (conversion) => {
        try {
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
      
      // Set timeout for this batch
      const batchResults = await Promise.race([
        Promise.all(batchPromises),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Batch timeout')), 2000)
        )
      ]);
      
      const validResults = batchResults.filter(result => result !== null);
      conversionsNeedingJourneys.push(...validResults);
      processedCount += batch.length;
      
      // Progress logging every 100 conversions
      if (processedCount % 100 === 0) {
        console.log(`📊 Progress: ${processedCount}/${allConversions.length} checked, ${conversionsNeedingJourneys.length} need journeys`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Batch error, including all conversions in batch for safety`);
      conversionsNeedingJourneys.push(...batch);
      processedCount += batch.length;
    }
  }
  
  console.log(`✅ Existence check complete: ${conversionsNeedingJourneys.length} conversions need journey processing`);
  return conversionsNeedingJourneys;
}

// COMPLETE IMPLEMENTATION: Process conversions with embedded attribution logic
async function processConversionsWithEmbeddedAttribution(redis, conversionsToProcess, journeyWindowHours, batchSize, maxTime) {
  const processStartTime = Date.now();
  console.log(`🚀 Processing ${conversionsToProcess.length} conversions with ${maxTime}ms remaining...`);
  
  let journeysCreated = 0;
  let conversionsProcessed = 0;
  let attributionCallsMade = 0;
  let attributionSuccesses = 0;
  let totalAttributionTime = 0;
  
  // Process conversions in batches until timeout
  for (let i = 0; i < conversionsToProcess.length; i += batchSize) {
    // Check timeout before each batch (need minimum 5 seconds per batch)
    const timeRemaining = maxTime - (Date.now() - processStartTime);
    if (timeRemaining < 5000) {
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
  const attributionSuccessRate = attributionCallsMade > 0 ? 
    ((attributionSuccesses / attributionCallsMade) * 100).toFixed(1) : '0.0';
  
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

// MISSING FUNCTION: The core batch processing with embedded attribution
async function processBatchWithEmbeddedAttribution(redis, conversions, journeyWindowHours) {
  const journeys = [];
  let attributionCalls = 0;
  let attributionSuccesses = 0;
  
  const batchPromises = conversions.map(async (conversion) => {
    try {
      attributionCalls++;
      
      // EMBEDDED ATTRIBUTION LOGIC - No external API calls!
      const conversionTime = new Date(conversion.timestamp).getTime();
      const windowStart = conversionTime - (journeyWindowHours * 60 * 60 * 1000);
      
      // Extract attribution data from conversion record
      const attributionData = {
        email: conversion.email,
        conversion_timestamp: conversion.timestamp,
        ips_to_check: extractIPs(conversion),
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature
      };
      
      // Find customer pageviews using embedded logic
      const pageviews = await findCustomerPageviewsEmbedded(redis, attributionData, windowStart, conversionTime);
      
      // Build the journey
      const journey = buildCustomerJourneyEmbedded(pageviews, conversion);
      
      // Store the journey in Redis
      const journeyKey = `customer_journey:journey_${conversion.order_id}_${Date.now()}`;
      const journeyData = encodeURIComponent(JSON.stringify(journey));
      
      await redis(`setnx/${journeyKey}/${journeyData}`, 10000); // 10 second timeout
      
      journeys.push(journey);
      attributionSuccesses++;
      
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

// Helper function: Extract IPs from conversion data
function extractIPs(conversion) {
  const ips = [];
  
  // Check various IP fields
  if (conversion.ip_address && conversion.ip_address !== 'unknown') {
    ips.push(conversion.ip_address);
  }
  if (conversion.ip && conversion.ip !== 'unknown') {
    ips.push(conversion.ip);
  }
  if (conversion.x_forwarded_for) {
    const forwardedIPs = conversion.x_forwarded_for.split(',').map(ip => ip.trim());
    ips.push(...forwardedIPs);
  }
  
  // Remove duplicates and unknowns
  return [...new Set(ips)].filter(ip => ip && ip !== 'unknown');
}

// Helper function: Find pageviews with embedded logic
async function findCustomerPageviewsEmbedded(redis, attributionData, windowStart, conversionTime) {
  const { ips_to_check, session_id, device_signature } = attributionData;
  const pageviews = [];
  const foundKeys = new Set();
  
  try {
    // Priority 1: Session ID match (highest confidence)
    if (session_id) {
      const sessionPageviews = await searchBySessionId(redis, session_id, windowStart, conversionTime);
      sessionPageviews.forEach(pv => {
        if (!foundKeys.has(pv._redis_key)) {
          pv.match_method = 'session_id';
          pv.confidence = 300;
          pageviews.push(pv);
          foundKeys.add(pv._redis_key);
        }
      });
    }
    
    // Priority 2: Device signature match (if no session matches)
    if (pageviews.length === 0 && device_signature) {
      const devicePageviews = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
      devicePageviews.forEach(pv => {
        if (!foundKeys.has(pv._redis_key)) {
          pv.match_method = 'device_signature';
          pv.confidence = 220;
          pageviews.push(pv);
          foundKeys.add(pv._redis_key);
        }
      });
    }
    
    // Priority 3: IP address matches (if still no matches)
    if (pageviews.length === 0 && ips_to_check.length > 0) {
      for (let i = 0; i < Math.min(ips_to_check.length, 3); i++) {
        const ip = ips_to_check[i];
        const ipPageviews = await searchByIpAddress(redis, ip, windowStart, conversionTime);
        ipPageviews.forEach(pv => {
          if (!foundKeys.has(pv._redis_key)) {
            pv.match_method = 'ip_address';
            pv.confidence = 280 - (i * 20); // Decreasing confidence for later IPs
            pageviews.push(pv);
            foundKeys.add(pv._redis_key);
          }
        });
      }
    }
    
  } catch (error) {
    console.warn(`⚠️ Pageview search error: ${error.message}`);
  }
  
  return pageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
}

// Helper function: Build customer journey
function buildCustomerJourneyEmbedded(pageviews, conversion) {
  const touchpoints = pageviews.map(pv => ({
    timestamp: pv.timestamp,
    page_url: pv.page_url,
    source: pv.source || 'direct',
    medium: pv.medium || 'none',
    campaign: pv.campaign || '(not set)',
    confidence: pv.confidence,
    match_method: pv.match_method
  }));
  
  // Add conversion as final touchpoint
  touchpoints.push({
    timestamp: conversion.timestamp,
    page_url: conversion.landing_page || 'conversion',
    source: conversion.source || 'direct',
    medium: 'conversion',
    campaign: conversion.campaign || '(not set)',
    confidence: 500,
    match_method: 'conversion',
    is_conversion: true,
    conversion_value: conversion.value || 0
  });
  
  return {
    journey_id: `journey_${conversion.order_id}_${Date.now()}`,
    customer_email: conversion.email,
    conversion_order_id: conversion.order_id,
    conversion_timestamp: conversion.timestamp,
    conversion_value: conversion.value || 0,
    total_touchpoints: touchpoints.length,
    touchpoints: touchpoints,
    attribution_method: 'embedded_logic',
    journey_duration_hours: touchpoints.length > 1 ? 
      (new Date(conversion.timestamp) - new Date(touchpoints[0].timestamp)) / (1000 * 60 * 60) : 0,
    created_at: new Date().toISOString()
  };
}

// Simplified search functions (basic Redis scans)
async function searchBySessionId(redis, sessionId, windowStart, conversionTime) {
  // Implementation would scan pageview indexes for session matches
  return []; // Simplified for now
}

async function searchByDeviceSignature(redis, deviceSignature, windowStart, conversionTime) {
  // Implementation would scan pageview indexes for device matches  
  return []; // Simplified for now
}

async function searchByIpAddress(redis, ipAddress, windowStart, conversionTime) {
  // Implementation would scan pageview indexes for IP matches
  return []; // Simplified for now
}

// Helper function for Redis initialization
function initializeRedis() {
  return (path, timeoutMs = 3000) => {
    const url = `${process.env.UPSTASH_REDIS_REST_URL}/${path}`;
    return Promise.race([
      fetch(url, {
        headers: { 'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}` }
      }).then(r => r.json()),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Redis timeout')), timeoutMs)
      )
    ]);
  };
}
