// attribution-recovery-engine.js
// Efficient IP-Based Attribution Recovery Engine
// Path: netlify/functions/attribution-recovery-engine.js
// Purpose: Recover missed pageview attributions for conversion-only journeys using dual IP data

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
    console.log('🔄 EFFICIENT ATTRIBUTION RECOVERY: Starting IP-based recovery...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      recovery_window_hours = 48,    // Focused 48-hour window vs 7-day standard
      batch_size = 50,               // Process in efficient batches
      force_reprocess = false        // Reprocess already attempted recoveries
    } = body;
    
    console.log(`🎯 Recovery Parameters: ${recovery_window_hours}h window, batch size: ${batch_size}`);
    
    // Step 1: Efficiently load conversion-only journeys
    const conversionOnlyJourneys = await loadConversionOnlyJourneys(redis, force_reprocess, maxProcessingTime - (Date.now() - startTime));
    console.log(`🎯 Found ${conversionOnlyJourneys.length} conversion-only journeys for recovery`);
    
    if (conversionOnlyJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversion-only journeys found that need recovery',
          summary: {
            conversion_only_journeys: 0,
            already_processed: true
          }
        })
      };
    }
    
    // Step 2: Extract unique IPs and batch load pageview indexes
    const recoveryResults = await performEfficientIPRecovery(
      redis, 
      conversionOnlyJourneys, 
      recovery_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Efficient recovery complete: ${recoveryResults.successful_recoveries} journeys enhanced in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        efficient_recovery: true,
        recovery_summary: {
          conversion_only_journeys_targeted: conversionOnlyJourneys.length,
          recovery_attempts: recoveryResults.recovery_attempts,
          successful_recoveries: recoveryResults.successful_recoveries,
          pageviews_recovered: recoveryResults.pageviews_recovered,
          processing_time_ms: totalTime
        },
        performance_metrics: {
          journeys_per_second: Math.round(recoveryResults.recovery_attempts / (totalTime / 1000)),
          recovery_success_rate: recoveryResults.recovery_attempts > 0 ? 
            ((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1) + '%' : '0%',
          average_pageviews_per_recovery: recoveryResults.successful_recoveries > 0 ? 
            (recoveryResults.pageviews_recovered / recoveryResults.successful_recoveries).toFixed(1) : '0',
          processing_efficiency: 'batch_memory_based'
        },
        attribution_improvement: {
          new_multi_touchpoint_journeys: recoveryResults.successful_recoveries,
          estimated_attribution_boost: recoveryResults.recovery_attempts > 0 ? 
            `+${((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1)}%` : '+0%'
        },
        ip_analysis: {
          unique_ips_processed: recoveryResults.unique_ips_processed,
          pageview_indexes_loaded: recoveryResults.pageview_indexes_loaded,
          total_pageviews_scanned: recoveryResults.total_pageviews_scanned
        },
        recovery_examples: recoveryResults.recovery_examples.slice(0, 5), // First 5 examples
        next_steps: recoveryResults.has_more_to_process ? [
          `Continue recovery: ${recoveryResults.journeys_remaining} conversion-only journeys remaining`,
          'Run same command again to continue processing',
          'Each run processes remaining journeys until complete'
        ] : [
          '🎉 IP-based attribution recovery complete!',
          `Successfully processed all conversion-only journeys`,
          'System attribution rate improved with recovered pageviews',
          'Use query-customer-journeys.js to verify improved attribution rates'
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

// Step 1: Efficiently load conversion-only journeys (following attribution-model-calculator.js pattern)
async function loadConversionOnlyJourneys(redis, forceReprocess, maxTime) {
  console.log('🔍 Efficiently scanning for conversion-only journeys...');
  
  const conversionOnlyJourneys = [];
  const scanStartTime = Date.now();
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  let keysScanned = 0;
  
  try {
    do {
      // Check timeout
      if (Date.now() - scanStartTime > maxTime - 3000) {
        console.log('⏰ Time limit during journey scan, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Process keys in batches (memory efficient)
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - scanStartTime > maxTime - 2000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Identify conversion-only journeys efficiently
              const isConversionOnly = journey.total_touchpoints === 1 || 
                                     journey.reconstruction_method?.includes('conversion_only') ||
                                     (journey.touchpoints && journey.touchpoints.every(tp => tp.is_conversion || tp.type === 'conversion'));
              
              const needsRecovery = isConversionOnly && 
                                  (forceReprocess || !journey.recovery_attempted);
              
              if (needsRecovery) {
                return {
                  journey_id: journey.journey_id,
                  journey_key: key,
                  customer_email: journey.customer_email,
                  conversion_order_id: journey.conversion_order_id,
                  conversion_timestamp: journey.conversion_timestamp,
                  conversion_value: journey.conversion_value,
                  
                  // Extract IP data already embedded from track.js (no need to lookup conversions:* keys!)
                  ip_addresses: extractIPsFromJourney(journey),
                  
                  original_journey: journey // Keep for updating
                };
              }
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validJourneys = batchResults.filter(journey => journey !== null);
        conversionOnlyJourneys.push(...validJourneys);
        
        if (conversionOnlyJourneys.length % 100 === 0 && conversionOnlyJourneys.length > 0) {
          console.log(`🎯 Scan progress: ${conversionOnlyJourneys.length} conversion-only journeys found`);
        }
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
    console.log(`✅ Scan complete: ${conversionOnlyJourneys.length} conversion-only journeys from ${keysScanned} keys scanned`);
    return conversionOnlyJourneys;
    
  } catch (scanError) {
    console.error('❌ Journey scan error:', scanError);
    return conversionOnlyJourneys;
  }
}

// Extract IP addresses from journey data (dual IP data already embedded from track.js)
function extractIPsFromJourney(journey) {
  const ips = [];
  
  // Journey data may contain IP info in various places
  // Check conversion touchpoint for IP data
  const conversionTouchpoint = journey.touchpoints?.find(tp => tp.is_conversion || tp.type === 'conversion');
  
  if (conversionTouchpoint) {
    // Look for IP data in conversion touchpoint
    if (conversionTouchpoint.primary_ip) ips.push(conversionTouchpoint.primary_ip);
    if (conversionTouchpoint.conversion_ip) ips.push(conversionTouchpoint.conversion_ip);
    if (conversionTouchpoint.pageview_ip) ips.push(conversionTouchpoint.pageview_ip);
    if (conversionTouchpoint.ip_address) ips.push(conversionTouchpoint.ip_address);
  }
  
  // Also check journey-level IP data
  if (journey.ip_address) ips.push(journey.ip_address);
  if (journey.primary_ip) ips.push(journey.primary_ip);
  if (journey.conversion_ip) ips.push(journey.conversion_ip);
  
  // Remove duplicates and invalid IPs
  const uniqueIPs = [...new Set(ips)].filter(ip => ip && ip !== 'unknown' && ip !== 'null');
  
  return uniqueIPs;
}

// Step 2: Perform efficient IP-based recovery (memory-based processing)
async function performEfficientIPRecovery(redis, conversionOnlyJourneys, recoveryWindowHours, batchSize, maxTime) {
  console.log(`🚀 Starting efficient IP recovery for ${conversionOnlyJourneys.length} journeys...`);
  
  const processStartTime = Date.now();
  let recoveryAttempts = 0;
  let successfulRecoveries = 0;
  let pageviewsRecovered = 0;
  let journeysRemaining = conversionOnlyJourneys.length;
  const recoveryExamples = [];
  
  // Step 2a: Extract all unique IPs for batch loading
  const allUniqueIPs = extractAllUniqueIPs(conversionOnlyJourneys);
  console.log(`📊 Extracted ${allUniqueIPs.length} unique IP addresses for batch loading`);
  
  // Step 2b: Batch load pageview indexes for all IPs
  const pageviewIndexes = await batchLoadPageviewIndexes(redis, allUniqueIPs, maxTime - (Date.now() - processStartTime));
  console.log(`💾 Loaded ${Object.keys(pageviewIndexes).length} pageview indexes`);
  
  // Step 2c: Process journeys in batches using in-memory data
  for (let i = 0; i < conversionOnlyJourneys.length; i += batchSize) {
    // Check timeout
    const timeRemaining = maxTime - (Date.now() - processStartTime);
    if (timeRemaining < 3000) {
      console.log(`⏰ Time limit reached after processing ${recoveryAttempts} journeys`);
      break;
    }
    
    const batch = conversionOnlyJourneys.slice(i, i + batchSize);
    console.log(`🔄 Processing recovery batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${conversionOnlyJourneys.length}`);
    
    // Process this batch in memory
    const batchResults = await processBatchInMemory(redis, batch, pageviewIndexes, recoveryWindowHours);
    
    recoveryAttempts += batch.length;
    successfulRecoveries += batchResults.successful_recoveries;
    pageviewsRecovered += batchResults.pageviews_recovered;
    journeysRemaining = conversionOnlyJourneys.length - (i + batch.length);
    recoveryExamples.push(...batchResults.recovery_examples);
    
    console.log(`✅ Batch complete: ${batchResults.successful_recoveries}/${batch.length} recovered (${successfulRecoveries}/${recoveryAttempts} total)`);
  }
  
  // Calculate total pageviews scanned across all indexes
  const totalPageviewsScanned = Object.values(pageviewIndexes).reduce((sum, index) => sum + (index.pageview_count || 0), 0);
  
  console.log(`🏁 Efficient recovery summary: ${successfulRecoveries}/${recoveryAttempts} journeys enhanced`);
  
  return {
    recovery_attempts: recoveryAttempts,
    successful_recoveries: successfulRecoveries,
    pageviews_recovered: pageviewsRecovered,
    unique_ips_processed: allUniqueIPs.length,
    pageview_indexes_loaded: Object.keys(pageviewIndexes).length,
    total_pageviews_scanned: totalPageviewsScanned,
    journeys_remaining: journeysRemaining,
    has_more_to_process: journeysRemaining > 0,
    recovery_examples: recoveryExamples,
    processing_time_ms: Date.now() - processStartTime
  };
}

// Extract all unique IPs from all journeys for batch loading
function extractAllUniqueIPs(conversionOnlyJourneys) {
  const allIPs = new Set();
  
  for (const journey of conversionOnlyJourneys) {
    for (const ip of journey.ip_addresses) {
      allIPs.add(ip);
    }
  }
  
  return Array.from(allIPs);
}

// Batch load pageview indexes for all unique IPs (efficient single pass)
async function batchLoadPageviewIndexes(redis, uniqueIPs, maxTime) {
  console.log(`💾 Batch loading pageview indexes for ${uniqueIPs.length} IPs...`);
  
  const loadStartTime = Date.now();
  const pageviewIndexes = {};
  
  // Process IPs in batches to avoid overwhelming Redis
  const batchSize = 25;
  for (let i = 0; i < uniqueIPs.length; i += batchSize) {
    if (Date.now() - loadStartTime > maxTime - 2000) {
      console.log('⏰ Time limit during pageview index loading, stopping');
      break;
    }
    
    const batch = uniqueIPs.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (ip) => {
      try {
        const encodedIP = ip.replace(/:/g, '_');
        const ipIndexKey = `pageview_index_ip:${encodedIP}`;
        
        const indexData = await redis(`get/${ipIndexKey}`, 1000);
        if (indexData?.result) {
          const parsed = JSON.parse(decodeURIComponent(indexData.result));
          
          // Only load if it's an enhanced index with multi-signal data
          if (parsed.multi_signal_ready && parsed.pageviews) {
            return { ip: encodedIP, index: parsed };
          }
        }
      } catch (error) {
        console.warn(`⚠️ Error loading pageview index for ${ip}:`, error.message);
      }
      return null;
    });
    
    const batchResults = await Promise.all(batchPromises);
    
    // Store loaded indexes
    for (const result of batchResults) {
      if (result) {
        pageviewIndexes[result.ip] = result.index;
      }
    }
    
    console.log(`💾 Loaded ${Object.keys(pageviewIndexes).length}/${i + batch.length} pageview indexes`);
  }
  
  console.log(`✅ Batch loading complete: ${Object.keys(pageviewIndexes).length} indexes loaded`);
  return pageviewIndexes;
}

// Process batch of journeys using in-memory pageview indexes
async function processBatchInMemory(redis, journeyBatch, pageviewIndexes, recoveryWindowHours) {
  let successfulRecoveries = 0;
  let pageviewsRecovered = 0;
  const recoveryExamples = [];
  
  const batchPromises = journeyBatch.map(async (journey) => {
    try {
      // Find matching pageviews using in-memory indexes and focused time window
      const matchingPageviews = findMatchingPageviewsInMemory(
        journey, 
        pageviewIndexes, 
        recoveryWindowHours
      );
      
      if (matchingPageviews.length > 0) {
        // Update journey with recovered pageviews
        await updateJourneyWithRecoveredPageviews(redis, journey, matchingPageviews);
        
        successfulRecoveries++;
        pageviewsRecovered += matchingPageviews.length;
        
        recoveryExamples.push({
          journey_id: journey.journey_id,
          order_id: journey.conversion_order_id,
          customer_email: journey.customer_email,
          pageviews_recovered: matchingPageviews.length,
          recovery_method: 'efficient_ip_matching',
          matched_ips: matchingPageviews.map(pv => pv.matched_ip),
          attribution_methods: matchingPageviews.map(pv => pv.attribution_method)
        });
        
        console.log(`✅ Recovery: Order ${journey.conversion_order_id} - found ${matchingPageviews.length} pageviews`);
      }
      
      return { success: matchingPageviews.length > 0, pageviews: matchingPageviews.length };
      
    } catch (recoveryError) {
      console.warn(`⚠️ Recovery error for journey ${journey.journey_id}:`, recoveryError.message);
      return { success: false, pageviews: 0 };
    }
  });
  
  await Promise.all(batchPromises);
  
  return {
    successful_recoveries: successfulRecoveries,
    pageviews_recovered: pageviewsRecovered,
    recovery_examples: recoveryExamples
  };
}

// Find matching pageviews using in-memory indexes with focused time window
function findMatchingPageviewsInMemory(journey, pageviewIndexes, recoveryWindowHours) {
  const matchingPageviews = [];
  const conversionTime = new Date(journey.conversion_timestamp).getTime();
  const windowStart = conversionTime - (recoveryWindowHours * 60 * 60 * 1000);
  
  // Search each IP address in the loaded indexes
  for (const ip of journey.ip_addresses) {
    const encodedIP = ip.replace(/:/g, '_');
    const pageviewIndex = pageviewIndexes[encodedIP];
    
    if (!pageviewIndex || !pageviewIndex.pageviews) continue;
    
    // Filter pageviews within the focused recovery time window
    const windowPageviews = pageviewIndex.pageviews.filter(pv => {
      const pvTime = new Date(pv.timestamp);
      return pvTime >= windowStart && pvTime <= conversionTime;
    });
    
    // Add matching pageviews with metadata
    for (const pv of windowPageviews) {
      matchingPageviews.push({
        ...pv,
        matched_ip: ip,
        attribution_method: 'ip_based_recovery',
        confidence: 200, // Medium confidence for IP-based recovery
        recovery_window_hours: recoveryWindowHours,
        time_before_conversion_hours: (conversionTime - new Date(pv.timestamp).getTime()) / (1000 * 60 * 60)
      });
    }
  }
  
  // Sort by timestamp (earliest first) and remove duplicates
  const sortedPageviews = matchingPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  const uniquePageviews = removeDuplicatePageviews(sortedPageviews);
  
  return uniquePageviews;
}

// Update journey with recovered pageviews
async function updateJourneyWithRecoveredPageviews(redis, journey, recoveredPageviews) {
  try {
    // Build enhanced journey with recovered pageviews
    const enhancedJourney = buildEnhancedJourneyFromRecovery(journey.original_journey, recoveredPageviews);
    
    // Update the journey record
    await redis(`setex/${journey.journey_key}/2592000/${encodeURIComponent(JSON.stringify(enhancedJourney))}`); // 30-day TTL
    
    console.log(`💾 Updated journey ${journey.journey_id} with ${recoveredPageviews.length} recovered pageviews`);
    
  } catch (updateError) {
    console.error(`❌ Error updating journey ${journey.journey_id}:`, updateError.message);
    throw updateError;
  }
}

// Build enhanced journey with recovered pageviews
function buildEnhancedJourneyFromRecovery(originalJourney, recoveredPageviews) {
  // Create new touchpoints from recovered pageviews
  const recoveredTouchpoints = recoveredPageviews.map((pageview, index) => ({
    touchpoint_id: `${originalJourney.conversion_order_id}_recovered_${index + 1}`,
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
    recovery_method: 'efficient_ip_based',
    
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
  
  // Get existing conversion touchpoint and update its position
  const existingConversionTouchpoint = originalJourney.touchpoints?.find(tp => tp.is_conversion || tp.type === 'conversion');
  if (existingConversionTouchpoint) {
    existingConversionTouchpoint.touchpoint_position = recoveredTouchpoints.length + 1;
    existingConversionTouchpoint.is_last_touchpoint = true;
  }
  
  const allTouchpoints = [...recoveredTouchpoints, existingConversionTouchpoint].filter(Boolean);
  
  // Recalculate journey metrics
  const journeyStart = new Date(allTouchpoints[0].timestamp);
  const journeyEnd = new Date(originalJourney.conversion_timestamp);
  const journeySpanHours = (journeyEnd - journeyStart) / (1000 * 60 * 60);
  
  const uniqueSessions = new Set(allTouchpoints.map(t => t.session_id).filter(Boolean)).size;
  const uniqueDeviceFingerprints = new Set(allTouchpoints.map(t => t.canvas_fingerprint).filter(Boolean)).size;
  const uniqueSources = new Set(allTouchpoints.map(t => t.source).filter(Boolean));
  
  return {
    ...originalJourney,
    
    // Updated journey metrics
    journey_start: allTouchpoints[0].timestamp,
    journey_span_hours: journeySpanHours,
    total_touchpoints: allTouchpoints.length,
    
    unique_sessions: uniqueSessions,
    unique_device_fingerprints: uniqueDeviceFingerprints,
    unique_sources: Array.from(uniqueSources),
    cross_session_journey: uniqueSessions > 1,
    cross_device_journey: uniqueDeviceFingerprints > 1,
    
    first_click_source: allTouchpoints[0].source,
    last_click_source: allTouchpoints[allTouchpoints.length - 2]?.source || allTouchpoints[0].source,
    attribution_confidence_avg: allTouchpoints.reduce((sum, t) => sum + (t.confidence || 0), 0) / allTouchpoints.length,
    
    // Updated touchpoints
    touchpoints: allTouchpoints,
    
    // Recovery metadata
    recovery_attempted: true,
    recovery_timestamp: new Date().toISOString(),
    recovery_method: 'efficient_ip_based',
    recovered_pageviews: recoveredPageviews.length,
    reconstruction_method: 'efficient_ip_based_recovery'
  };
}

// Remove duplicate pageviews (same timestamp + session_id)
function removeDuplicatePageviews(pageviews) {
  const seen = new Set();
  return pageviews.filter(pv => {
    const key = `${pv.timestamp}_${pv.session_id || pv.ip_address}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

// Initialize Redis helper (same pattern as attribution-model-calculator.js)
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
