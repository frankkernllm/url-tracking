// attribution-recovery-engine.js
// SERVERLESS Attribution Recovery Engine - Uses Proven Conversion Loading Logic
// Path: netlify/functions/attribution-recovery-engine.js
// Purpose: Recover missed attributions using proven serverless approach from build-customer-journeys.js

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
    console.log('🚀 SERVERLESS ATTRIBUTION RECOVERY: Using proven conversion loading logic...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      extended_window_hours = 72,    // 72-hour extended window vs 7-day standard
      batch_size = 20,               // Batch size for processing
      limit_conversions = 500        // Limit conversions to process (to avoid timeout)
    } = body;
    
    console.log(`🎯 Serverless Recovery Parameters: ${extended_window_hours}h window, batch size: ${batch_size}, limit: ${limit_conversions}`);
    
    // Step 1: Load conversions using proven serverless approach (from build-customer-journeys.js)
    const allConversions = await loadAllConversionsServerless(redis, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Loaded ${allConversions.length} total conversions using proven serverless approach`);
    
    if (allConversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversions found in database',
          summary: {
            conversions_found: 0,
            recovery_approach: 'proven_serverless_loading'
          }
        })
      };
    }
    
    // Step 2: Filter for conversions needing attribution recovery
    const conversionsNeedingRecovery = filterConversionsNeedingRecovery(allConversions, limit_conversions);
    console.log(`🎯 Found ${conversionsNeedingRecovery.length} conversions needing attribution recovery`);
    
    if (conversionsNeedingRecovery.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversions found that need attribution recovery',
          summary: {
            total_conversions_found: allConversions.length,
            conversions_needing_recovery: 0,
            recovery_approach: 'proven_serverless_loading'
          }
        })
      };
    }
    
    // Step 3: Process recovery using existing pageview indexes
    const recoveryResults = await processServerlessAttributionRecovery(
      redis, 
      conversionsNeedingRecovery, 
      extended_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Serverless recovery complete: ${recoveryResults.successful_recoveries} recoveries in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        serverless_recovery: true,
        recovery_summary: {
          total_conversions_found: allConversions.length,
          conversions_needing_recovery: conversionsNeedingRecovery.length,
          recovery_attempts: recoveryResults.recovery_attempts,
          successful_recoveries: recoveryResults.successful_recoveries,
          additional_pageviews_found: recoveryResults.additional_pageviews_found,
          processing_time_ms: totalTime,
          recovery_approach: 'proven_serverless_loading'
        },
        recovery_performance: {
          recovery_success_rate: recoveryResults.recovery_attempts > 0 ? 
            ((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1) + '%' : '0%',
          average_pageviews_per_recovery: recoveryResults.successful_recoveries > 0 ? 
            (recoveryResults.additional_pageviews_found / recoveryResults.successful_recoveries).toFixed(1) : '0',
          processing_efficiency: 'proven_serverless_from_build_customer_journeys',
          conversions_per_second: Math.round(recoveryResults.recovery_attempts / (totalTime / 1000))
        },
        attribution_improvements: {
          new_multi_touchpoint_journeys: recoveryResults.successful_recoveries,
          estimated_attribution_rate_improvement: recoveryResults.recovery_attempts > 0 ? 
            `+${((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1)}%` : '+0%'
        },
        recovery_details: recoveryResults.recovery_details.slice(0, 10), // First 10 examples
        data_sources_used: {
          conversion_source: 'conversions:* keys (direct)',
          pageview_indexes: 'pageview_index_ip:* keys',
          loading_method: 'proven_serverless_from_build_customer_journeys'
        },
        next_steps: recoveryResults.successful_recoveries > 0 ? [
          `🎉 Successfully recovered ${recoveryResults.successful_recoveries} attribution matches!`,
          'Conversions now have multi-touchpoint customer journeys',
          'Run query-customer-journeys.js to see improved attribution rates',
          'Use attribution-model-calculator.js for multi-touch attribution analysis'
        ] : [
          'No additional attributions found using serverless approach',
          'Current pageview indexes may not contain matches for these conversions',
          'Consider running extract-pageviews-chunked.js to refresh pageview data',
          'System has processed all available attribution opportunities'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Serverless attribution recovery failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Serverless attribution recovery failed', 
        message: error.message 
      })
    };
  }
};

// PROVEN: Load ALL conversions using serverless approach (from build-customer-journeys.js)
async function loadAllConversionsServerless(redis, maxTime) {
  console.log(`🔍 Loading ALL conversions using proven serverless approach (no date limits)...`);
  
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
              
              // Include ALL conversions that have required fields (same logic as build-customer-journeys.js)
              if (conversion.timestamp && conversion.email && conversion.order_id) {
                return {
                  email: conversion.email,
                  timestamp: conversion.timestamp,
                  order_id: conversion.order_id,
                  order_total: conversion.order_total || 0,
                  
                  // Attribution signals for embedded attribution logic (from build-customer-journeys.js)
                  session_id: conversion.session_id,
                  device_signature: conversion.device_signature || conversion.dsig,
                  screen_value: conversion.screen_value || conversion.SVV || conversion.SVVV,
                  gpu_signature: conversion.gpu_signature || conversion.gsig,
                  
                  // IP addresses for multi-IP attribution (from build-customer-journeys.js)
                  primary_ip: conversion.primary_ip,
                  conversion_ip: conversion.conversion_ip,
                  pageview_ip: conversion.pageview_ip,
                  ip_addresses: [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean),
                  
                  // Current attribution (for comparison) (from build-customer-journeys.js)
                  current_attribution_found: conversion.attribution_found,
                  current_attribution_method: conversion.attribution_method,
                  current_source: conversion.source,
                  current_landing_page: conversion.landing_page,
                  
                  // Additional recovery-specific fields
                  dual_ip_scenario: conversion.dual_ip_scenario,
                  ip_addresses_detected: conversion.ip_addresses_detected,
                  
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
  
  console.log(`✅ Loaded ${conversions.length} total conversions from database using proven approach`);
  return conversions;
}

// Filter conversions that need attribution recovery
function filterConversionsNeedingRecovery(allConversions, limit) {
  console.log(`🔍 Filtering conversions needing recovery from ${allConversions.length} total conversions...`);
  
  const conversionsNeedingRecovery = [];
  
  for (const conversion of allConversions) {
    // Conversion needs recovery if:
    // 1. No attribution found, OR
    // 2. Attribution method is none/direct, OR  
    // 3. Source is direct/unknown, OR
    // 4. No landing page data
    const needsRecovery = !conversion.current_attribution_found || 
                         conversion.current_attribution_method === 'none' ||
                         conversion.current_attribution_method === 'direct' ||
                         conversion.current_source === 'direct' ||
                         conversion.current_source === 'unknown' ||
                         !conversion.current_landing_page ||
                         conversion.current_landing_page === 'null' ||
                         conversion.current_landing_page === 'unknown';
    
    if (needsRecovery) {
      // Only include if we have IP addresses to work with
      if (conversion.ip_addresses && conversion.ip_addresses.length > 0) {
        conversionsNeedingRecovery.push(conversion);
        
        if (conversionsNeedingRecovery.length >= limit) {
          break; // Respect the limit to avoid timeout
        }
      }
    }
  }
  
  console.log(`✅ Found ${conversionsNeedingRecovery.length} conversions needing recovery (limited to ${limit})`);
  console.log(`📊 Recovery criteria: no attribution, direct/unknown source, or missing landing page`);
  console.log(`📊 Recovery requirement: must have IP addresses available for pageview matching`);
  
  return conversionsNeedingRecovery;
}

// Process attribution recovery using existing pageview indexes
async function processServerlessAttributionRecovery(redis, conversions, extendedWindowHours, batchSize, maxTime) {
  console.log(`🚀 Processing ${conversions.length} conversions using existing pageview indexes...`);
  
  const processStartTime = Date.now();
  let recoveryAttempts = 0;
  let successfulRecoveries = 0;
  let additionalPageviewsFound = 0;
  const recoveryDetails = [];
  
  // Process conversions in batches
  for (let i = 0; i < conversions.length; i += batchSize) {
    // Check timeout
    const timeRemaining = maxTime - (Date.now() - processStartTime);
    if (timeRemaining < 3000) {
      console.log(`⏰ Time limit reached after processing ${recoveryAttempts} conversions`);
      break;
    }
    
    const batch = conversions.slice(i, i + batchSize);
    console.log(`🔄 Processing batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${conversions.length}`);
    
    // Process this batch using existing indexes
    const batchResults = await processBatchUsingPageviewIndexes(redis, batch, extendedWindowHours);
    
    recoveryAttempts += batch.length;
    successfulRecoveries += batchResults.successful_recoveries;
    additionalPageviewsFound += batchResults.additional_pageviews_found;
    recoveryDetails.push(...batchResults.recovery_details);
    
    console.log(`✅ Batch complete: ${batchResults.successful_recoveries}/${batch.length} recovered (${recoveryAttempts}/${conversions.length} total)`);
  }
  
  console.log(`🏁 Serverless recovery summary: ${successfulRecoveries}/${recoveryAttempts} conversions recovered`);
  
  return {
    recovery_attempts: recoveryAttempts,
    successful_recoveries: successfulRecoveries,
    additional_pageviews_found: additionalPageviewsFound,
    recovery_details: recoveryDetails,
    processing_time_ms: Date.now() - processStartTime
  };
}

// Process batch using existing pageview indexes
async function processBatchUsingPageviewIndexes(redis, batch, extendedWindowHours) {
  let successfulRecoveries = 0;
  let additionalPageviewsFound = 0;
  const recoveryDetails = [];
  
  const batchPromises = batch.map(async (conversion) => {
    try {
      const recoveryStartTime = Date.now();
      
      console.log(`🔍 Recovery attempt for order ${conversion.order_id}: ${conversion.ip_addresses.length} IPs available`);
      
      // Query existing pageview indexes with the IP data from serverless loading
      const recoveredPageviews = await queryPageviewIndexesForRecovery(redis, {
        conversion_timestamp: conversion.timestamp,
        ip_addresses: conversion.ip_addresses,
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature,
        window_hours: extendedWindowHours
      });
      
      if (recoveredPageviews && recoveredPageviews.length > 0) {
        // Create recovered journey record
        await createRecoveredJourneyRecord(redis, conversion, recoveredPageviews);
        
        successfulRecoveries++;
        additionalPageviewsFound += recoveredPageviews.length;
        
        recoveryDetails.push({
          order_id: conversion.order_id,
          customer_email: conversion.email,
          pageviews_recovered: recoveredPageviews.length,
          recovery_method: 'serverless_pageview_index_query',
          ips_used: conversion.ip_addresses,
          current_attribution: conversion.current_attribution_method,
          current_source: conversion.current_source,
          recovery_time_ms: Date.now() - recoveryStartTime,
          attribution_methods: recoveredPageviews.map(pv => pv.attribution_method),
          highest_confidence: Math.max(...recoveredPageviews.map(pv => pv.confidence || 0))
        });
        
        console.log(`✅ Recovery success: Order ${conversion.order_id} - found ${recoveredPageviews.length} pageviews (was: ${conversion.current_source})`);
      } else {
        console.log(`❌ No recovery: Order ${conversion.order_id} - no pageviews found in ${extendedWindowHours}h window`);
      }
      
      return { success: recoveredPageviews.length > 0, pageviews: recoveredPageviews.length };
      
    } catch (recoveryError) {
      console.warn(`⚠️ Recovery error for order ${conversion.order_id}:`, recoveryError.message);
      return { success: false, pageviews: 0 };
    }
  });
  
  await Promise.all(batchPromises);
  
  return {
    successful_recoveries: successfulRecoveries,
    additional_pageviews_found: additionalPageviewsFound,
    recovery_details: recoveryDetails
  };
}

// Query pageview indexes for recovery
async function queryPageviewIndexesForRecovery(redis, params) {
  const { conversion_timestamp, ip_addresses, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  const windowEnd = conversionTime;
  
  let allMatches = [];
  
  try {
    console.log(`🔍 Querying pageview indexes: ${ip_addresses.length} IPs, ${window_hours}h window`);
    
    // Query existing pageview indexes for each IP
    for (const ip of ip_addresses) {
      if (!ip || ip === 'unknown') continue;
      
      const encodedIP = ip.replace(/:/g, '_');
      const ipIndexKey = `pageview_index_ip:${encodedIP}`;
      
      try {
        const indexData = await redis(`get/${ipIndexKey}`);
        
        if (indexData?.result) {
          const parsed = JSON.parse(decodeURIComponent(indexData.result));
          
          if (parsed.multi_signal_ready && parsed.pageviews) {
            // Filter pageviews within time window
            const windowPageviews = parsed.pageviews.filter(pv => {
              const pvTime = new Date(pv.timestamp);
              return pvTime >= windowStart && pvTime <= windowEnd;
            });
            
            console.log(`📊 IP ${ip}: ${windowPageviews.length} pageviews in time window`);
            
            // Apply multi-signal matching
            for (const pv of windowPageviews) {
              let confidence = 240; // Base IP match confidence
              let attributionMethod = 'ip_index_recovery';
              
              // PRIORITY 1: Session ID match (highest confidence)
              if (session_id && pv.session_id === session_id) {
                confidence = 295;
                attributionMethod = 'session_id_recovery';
              }
              // PRIORITY 2: Device signature match
              else if (device_signature && pv.canvas_fingerprint === device_signature) {
                confidence = 255;
                attributionMethod = 'device_signature_recovery';
              }
              // PRIORITY 3: Screen signature match
              else if (screen_value && pv.screen_resolution && hashString(pv.screen_resolution) === screen_value) {
                confidence = 195;
                attributionMethod = 'screen_signature_recovery';
              }
              // PRIORITY 4: GPU signature match
              else if (gpu_signature && pv.webgl_fingerprint && hashString(pv.webgl_fingerprint) === gpu_signature) {
                confidence = 175;
                attributionMethod = 'webgl_signature_recovery';
              }
              
              allMatches.push({
                ...pv,
                matched_ip: ip,
                attribution_method: attributionMethod,
                confidence: confidence,
                recovery_source: 'pageview_index'
              });
            }
          }
        }
        
      } catch (indexError) {
        console.warn(`⚠️ Error querying pageview index for IP ${ip}:`, indexError.message);
      }
    }
    
    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
    
    console.log(`✅ Index query complete: ${uniqueMatches.length} unique pageviews found`);
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Pageview index query error:', error.message);
    return [];
  }
}

// Create recovered journey record
async function createRecoveredJourneyRecord(redis, conversion, recoveredPageviews) {
  // Sort pageviews by timestamp
  const sortedPageviews = recoveredPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Create touchpoints from recovered pageviews
  const touchpoints = sortedPageviews.map((pageview, index) => ({
    touchpoint_id: `${conversion.order_id}_recovered_${index + 1}`,
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
    recovery_method: 'serverless_pageview_index_query',
    
    // Session and device data
    session_id: pageview.session_id,
    canvas_fingerprint: pageview.canvas_fingerprint,
    screen_resolution: pageview.screen_resolution,
    user_agent: pageview.user_agent,
    
    // Position in journey
    touchpoint_position: index + 1,
    is_first_touchpoint: index === 0,
    is_last_touchpoint: false
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
  
  const recoveredJourney = {
    journey_id: `journey_${conversion.order_id}_serverless_recovery_${Date.now()}`,
    customer_email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    conversion_order_id: conversion.order_id,
    conversion_value: conversion.order_total,
    
    journey_start: touchpoints[0].timestamp,
    journey_end: conversion.timestamp,
    journey_span_hours: journeySpanHours,
    total_touchpoints: touchpoints.length,
    
    unique_sessions: uniqueSessions,
    unique_device_fingerprints: uniqueDeviceFingerprints,
    unique_sources: Array.from(uniqueSources),
    cross_session_journey: uniqueSessions > 1,
    cross_device_journey: uniqueDeviceFingerprints > 1,
    
    first_click_source: touchpoints[0].source,
    last_click_source: touchpoints[touchpoints.length - 2]?.source || touchpoints[0].source,
    attribution_confidence_avg: touchpoints.reduce((sum, t) => sum + (t.confidence || 0), 0) / touchpoints.length,
    
    touchpoints: touchpoints,
    
    // Recovery metadata
    recovery_attempted: true,
    recovery_timestamp: new Date().toISOString(),
    recovery_method: 'serverless_pageview_index_query',
    recovered_pageviews: sortedPageviews.length,
    reconstruction_method: 'serverless_attribution_recovery',
    original_attribution_method: conversion.current_attribution_method,
    original_source: conversion.current_source,
    
    created_at: new Date().toISOString()
  };
  
  // Store the recovered journey
  const journeyKey = `customer_journey:${recoveredJourney.journey_id}`;
  await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(recoveredJourney))}`); // 30-day TTL
  
  console.log(`💾 Created serverless recovered journey: ${recoveredJourney.journey_id} with ${sortedPageviews.length} pageviews`);
}

// Helper functions

// Hash function for privacy-safe parameter values
function hashString(str) {
  if (!str) return '';
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(36);
}

// Remove duplicate matches
function removeDuplicateMatches(matches) {
  const seen = new Set();
  return matches.filter(match => {
    const key = `${match.timestamp}_${match.session_id || match.ip_address}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
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
