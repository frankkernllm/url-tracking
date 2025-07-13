// attribution-recovery-engine.js
// EFFICIENT Attribution Recovery Engine - Uses Existing Processed Data
// Path: netlify/functions/attribution-recovery-engine.js
// Purpose: Recover missed attributions using existing conversion and pageview indexes

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
    console.log('🚀 EFFICIENT ATTRIBUTION RECOVERY: Using existing processed data...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      extended_window_hours = 72,    // 72-hour extended window vs 7-day standard
      batch_size = 20,               // Larger batches since we're more efficient
      date_range_days = 40           // Look back 40 days for conversions
    } = body;
    
    console.log(`🎯 Efficient Recovery Parameters: ${extended_window_hours}h window, ${date_range_days} days lookback, batch size: ${batch_size}`);
    
    // Step 1: Load conversions needing recovery from processed data
    const conversionsNeedingRecovery = await loadConversionsNeedingRecovery(redis, date_range_days, maxProcessingTime - (Date.now() - startTime));
    console.log(`📊 Found ${conversionsNeedingRecovery.length} conversions needing attribution recovery`);
    
    if (conversionsNeedingRecovery.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversions found that need attribution recovery',
          summary: {
            conversions_needing_recovery: 0,
            recovery_approach: 'efficient_processed_data'
          }
        })
      };
    }
    
    // Step 2: Process recovery using existing pageview indexes
    const recoveryResults = await processEfficientAttributionRecovery(
      redis, 
      conversionsNeedingRecovery, 
      extended_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Efficient recovery complete: ${recoveryResults.successful_recoveries} recoveries in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        efficient_recovery: true,
        recovery_summary: {
          conversions_needing_recovery: conversionsNeedingRecovery.length,
          recovery_attempts: recoveryResults.recovery_attempts,
          successful_recoveries: recoveryResults.successful_recoveries,
          additional_pageviews_found: recoveryResults.additional_pageviews_found,
          processing_time_ms: totalTime,
          recovery_approach: 'efficient_processed_data'
        },
        recovery_performance: {
          recovery_success_rate: recoveryResults.recovery_attempts > 0 ? 
            ((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1) + '%' : '0%',
          average_pageviews_per_recovery: recoveryResults.successful_recoveries > 0 ? 
            (recoveryResults.additional_pageviews_found / recoveryResults.successful_recoveries).toFixed(1) : '0',
          processing_efficiency: 'uses_existing_indexes',
          conversions_per_second: Math.round(recoveryResults.recovery_attempts / (totalTime / 1000))
        },
        attribution_improvements: {
          new_multi_touchpoint_journeys: recoveryResults.successful_recoveries,
          estimated_attribution_rate_improvement: recoveryResults.recovery_attempts > 0 ? 
            `+${((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1)}%` : '+0%'
        },
        recovery_details: recoveryResults.recovery_details.slice(0, 10), // First 10 examples
        data_sources_used: {
          conversion_indexes: 'conversion_index_date:*',
          pageview_indexes: 'pageview_index_ip:*',
          ip_analytics: 'ip_analytics:*'
        },
        next_steps: recoveryResults.successful_recoveries > 0 ? [
          `🎉 Successfully recovered ${recoveryResults.successful_recoveries} attribution matches!`,
          'Conversions now have multi-touchpoint customer journeys',
          'Run query-customer-journeys.js to see improved attribution rates',
          'Use attribution-model-calculator.js for multi-touch attribution analysis'
        ] : [
          'No additional attributions found using existing processed data',
          'Current pageview indexes may not contain matches for these conversions',
          'Consider running extract-pageviews-chunked.js to refresh pageview data',
          'System has processed all available attribution opportunities'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Efficient attribution recovery failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Efficient attribution recovery failed', 
        message: error.message 
      })
    };
  }
};

// EFFICIENT: Load conversions needing recovery from processed data
async function loadConversionsNeedingRecovery(redis, dateRangeDays, maxTime) {
  console.log(`📊 Loading conversions from processed indexes (${dateRangeDays} days)...`);
  
  const loadStartTime = Date.now();
  const conversionsNeedingRecovery = [];
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - dateRangeDays);
  
  try {
    // Generate date keys for the lookback period
    const dateKeys = generateDateKeys(cutoffDate, new Date());
    console.log(`📅 Checking ${dateKeys.length} date indexes for conversions needing recovery`);
    
    // Load from each date index
    for (const dateKey of dateKeys) {
      if (Date.now() - loadStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during conversion loading, stopping');
        break;
      }
      
      const indexKey = `conversion_index_date:${dateKey}`;
      
      try {
        const indexData = await redis(`get/${indexKey}`);
        
        if (indexData?.result) {
          const parsed = JSON.parse(decodeURIComponent(indexData.result));
          
          if (parsed.conversions && Array.isArray(parsed.conversions)) {
            console.log(`📊 ${dateKey}: ${parsed.conversions.length} conversions found`);
            
            // Filter for conversions needing recovery
            for (const conversion of parsed.conversions) {
              // Conversion needs recovery if:
              // 1. No attribution found, OR
              // 2. Attribution method is conversion_only/direct
              const needsRecovery = !conversion.attribution_found || 
                                  conversion.attribution_method === 'none' ||
                                  conversion.source === 'direct' ||
                                  !conversion.landing_page ||
                                  conversion.landing_page === 'null';
              
              if (needsRecovery && conversion.timestamp && conversion.email) {
                conversionsNeedingRecovery.push({
                  email: conversion.email,
                  timestamp: conversion.timestamp,
                  order_id: conversion.order_id,
                  order_total: conversion.order_total || 0,
                  
                  // Use existing processed IP data (this is the key improvement!)
                  processed_ips: {
                    primary_ip: conversion.primary_ip,
                    conversion_ip: conversion.conversion_ip,
                    pageview_ip: conversion.pageview_ip,
                    all_ips: [conversion.primary_ip, conversion.conversion_ip, conversion.pageview_ip].filter(Boolean)
                  },
                  
                  // Attribution signals
                  session_id: conversion.session_id,
                  device_signature: conversion.device_signature || conversion.dsig,
                  screen_value: conversion.screen_value || conversion.SVV || conversion.SVVV,
                  gpu_signature: conversion.gpu_signature || conversion.gsig,
                  
                  // Current attribution status
                  current_attribution_found: conversion.attribution_found,
                  current_attribution_method: conversion.attribution_method,
                  current_source: conversion.source,
                  current_landing_page: conversion.landing_page,
                  
                  // Metadata
                  date_key: dateKey,
                  dual_ip_scenario: conversion.dual_ip_scenario
                });
              }
            }
          }
        } else {
          console.log(`📊 ${dateKey}: No conversion index found`);
        }
        
      } catch (parseError) {
        console.warn(`⚠️ Error loading conversion index for ${dateKey}:`, parseError.message);
      }
    }
    
  } catch (loadError) {
    console.error('❌ Error loading conversions from indexes:', loadError);
  }
  
  // Sort by timestamp (most recent first)
  conversionsNeedingRecovery.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`✅ Loaded ${conversionsNeedingRecovery.length} conversions needing recovery from processed data`);
  return conversionsNeedingRecovery;
}

// Generate date keys for the lookback period
function generateDateKeys(startDate, endDate) {
  const keys = [];
  const current = new Date(startDate);
  
  while (current <= endDate) {
    const dateKey = `${current.getFullYear()}-${String(current.getMonth() + 1).padStart(2, '0')}-${String(current.getDate()).padStart(2, '0')}`;
    keys.push(dateKey);
    current.setDate(current.getDate() + 1);
  }
  
  return keys;
}

// EFFICIENT: Process attribution recovery using existing pageview indexes
async function processEfficientAttributionRecovery(redis, conversions, extendedWindowHours, batchSize, maxTime) {
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
    const batchResults = await processBatchUsingIndexes(redis, batch, extendedWindowHours);
    
    recoveryAttempts += batch.length;
    successfulRecoveries += batchResults.successful_recoveries;
    additionalPageviewsFound += batchResults.additional_pageviews_found;
    recoveryDetails.push(...batchResults.recovery_details);
    
    console.log(`✅ Batch complete: ${batchResults.successful_recoveries}/${batch.length} recovered (${recoveryAttempts}/${conversions.length} total)`);
  }
  
  console.log(`🏁 Efficient recovery summary: ${successfulRecoveries}/${recoveryAttempts} conversions recovered`);
  
  return {
    recovery_attempts: recoveryAttempts,
    successful_recoveries: successfulRecoveries,
    additional_pageviews_found: additionalPageviewsFound,
    recovery_details: recoveryDetails,
    processing_time_ms: Date.now() - processStartTime
  };
}

// Process batch using existing pageview indexes
async function processBatchUsingIndexes(redis, batch, extendedWindowHours) {
  let successfulRecoveries = 0;
  let additionalPageviewsFound = 0;
  const recoveryDetails = [];
  
  const batchPromises = batch.map(async (conversion) => {
    try {
      const recoveryStartTime = Date.now();
      
      // Use existing processed IP data and pageview indexes
      const recoveredPageviews = await queryExistingPageviewIndexes(redis, {
        conversion_timestamp: conversion.timestamp,
        processed_ips: conversion.processed_ips.all_ips,
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature,
        window_hours: extendedWindowHours
      });
      
      if (recoveredPageviews && recoveredPageviews.length > 0) {
        // Create/update journey record with recovered attribution
        await createRecoveredJourneyRecord(redis, conversion, recoveredPageviews);
        
        successfulRecoveries++;
        additionalPageviewsFound += recoveredPageviews.length;
        
        recoveryDetails.push({
          order_id: conversion.order_id,
          customer_email: conversion.email,
          pageviews_recovered: recoveredPageviews.length,
          recovery_method: 'existing_pageview_indexes',
          ips_used: conversion.processed_ips.all_ips,
          recovery_time_ms: Date.now() - recoveryStartTime,
          attribution_methods: recoveredPageviews.map(pv => pv.attribution_method),
          highest_confidence: Math.max(...recoveredPageviews.map(pv => pv.confidence || 0))
        });
        
        console.log(`✅ Recovery success: Order ${conversion.order_id} - found ${recoveredPageviews.length} pageviews`);
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

// EFFICIENT: Query existing pageview indexes directly
async function queryExistingPageviewIndexes(redis, params) {
  const { conversion_timestamp, processed_ips, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  const windowEnd = conversionTime;
  
  let allMatches = [];
  
  try {
    console.log(`🔍 Querying pageview indexes: ${processed_ips.length} IPs, ${window_hours}h window`);
    
    // Query existing pageview indexes for each IP
    for (const ip of processed_ips) {
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
    recovery_method: 'efficient_index_query',
    
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
    journey_id: `journey_${conversion.order_id}_recovered_${Date.now()}`,
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
    recovery_method: 'efficient_pageview_index_query',
    recovered_pageviews: sortedPageviews.length,
    reconstruction_method: 'efficient_attribution_recovery',
    
    created_at: new Date().toISOString()
  };
  
  // Store the recovered journey
  const journeyKey = `customer_journey:${recoveredJourney.journey_id}`;
  await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(recoveredJourney))}`); // 30-day TTL
  
  console.log(`💾 Created recovered journey: ${recoveredJourney.journey_id} with ${sortedPageviews.length} pageviews`);
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
