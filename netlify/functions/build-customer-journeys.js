// netlify/functions/build-customer-journeys.js
// FIXED: Proper stateless resume with correct field names and chronological processing

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
    console.log('🏗️ FIXED: Customer Journey Builder Starting...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      batch_size = 100,
      reset_progress = false,
      target_email = null,
      max_conversions = null
    } = body;
    
    // FIXED: Get proper conversion/journey analysis
    const analysis = await getConversionJourneyAnalysis(redis, reset_progress);
    
    if (!reset_progress && analysis.remaining_conversions === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          status: 'all_journeys_complete',
          total_conversions: analysis.total_conversions,
          journeys_built: analysis.existing_journeys,
          message: '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          processing_time_ms: Date.now() - startTime,
          completion_percentage: '100.0%'
        })
      };
    }
    
    console.log(`📊 ANALYSIS: ${analysis.total_conversions} total conversions, ${analysis.existing_journeys} complete journeys, ${analysis.remaining_conversions} remaining (includes broken journeys to reprocess)`);
    
    // FIXED: Load remaining conversions in chronological order (includes broken journeys for reprocessing)
    const conversionsToProcess = await loadRemainingConversions(
      redis, 
      analysis.processed_order_ids, 
      target_email, 
      max_conversions || batch_size
    );
    
    if (conversionsToProcess.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          status: 'no_remaining_conversions',
          message: 'No remaining conversions to process.',
          total_conversions: analysis.total_conversions,
          journeys_built: analysis.existing_journeys,
          processing_time_ms: Date.now() - startTime,
          completion_percentage: ((analysis.existing_journeys / analysis.total_conversions) * 100).toFixed(1) + '%'
        })
      };
    }
    
    console.log(`🔨 Processing ${conversionsToProcess.length} conversions (${conversionsToProcess[0]?.timestamp} to ${conversionsToProcess[conversionsToProcess.length-1]?.timestamp})`);
    
    // Process conversions with proper attribution
    let totalJourneysBuilt = 0;
    let totalAttributionCalls = 0;
    let totalAttributionSuccesses = 0;
    
    for (let i = 0; i < conversionsToProcess.length; i += Math.min(batch_size, 50)) {
      if (Date.now() - startTime > maxProcessingTime - 3000) {
        console.log('⏰ Time limit approaching, stopping build process');
        break;
      }
      
      const batch = conversionsToProcess.slice(i, i + Math.min(batch_size, 50));
      console.log(`🔨 Processing batch ${Math.floor(i/50) + 1}: ${batch.length} conversions`);
      
      const batchResult = await processBatchFixed(redis, batch);
      
      totalJourneysBuilt += batchResult.journeys.length;
      totalAttributionCalls += batchResult.attribution_calls;
      totalAttributionSuccesses += batchResult.attribution_successes;
      
      // Store journeys
      for (const journey of batchResult.journeys) {
        const journeyKey = `customer_journey:${journey.journey_id}`;
        await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
      }
    }
    
    const totalTime = Date.now() - startTime;
    const newTotalJourneys = analysis.existing_journeys + totalJourneysBuilt;
    const completionPercentage = ((newTotalJourneys / analysis.total_conversions) * 100).toFixed(1);
    const conversionsRemaining = analysis.total_conversions - newTotalJourneys;
    
    console.log(`✅ BATCH COMPLETE: ${totalJourneysBuilt} new journeys built in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        status: conversionsRemaining === 0 ? 'all_journeys_complete' : 'batch_complete',
        batch_summary: {
          conversions_processed: totalJourneysBuilt,
          attribution_calls: totalAttributionCalls,
          attribution_successes: totalAttributionSuccesses,
          attribution_success_rate: totalAttributionCalls > 0 ? 
            ((totalAttributionSuccesses / totalAttributionCalls) * 100).toFixed(1) + '%' : '0%',
          processing_time_ms: totalTime,
          journeys_per_second: Math.round(totalJourneysBuilt / (totalTime / 1000))
        },
        progress: {
          total_conversions: analysis.total_conversions,
          journeys_built: newTotalJourneys,
          conversions_remaining: conversionsRemaining,
          completion_percentage: completionPercentage + '%'
        },
        next_steps: conversionsRemaining === 0 ? [
          '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          'System ready for comprehensive multi-touch attribution analysis',
          'Use query-customer-journeys.js for business intelligence reports'
        ] : [
          `Continue processing: ${conversionsRemaining} conversions remaining`,
          'Run the same command again to continue automatically',
          `Estimated runs remaining: ${Math.ceil(conversionsRemaining / batch_size)}`
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Journey building failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Journey building failed', 
        message: error.message
      })
    };
  }
};

// FIXED: Proper analysis of conversions vs existing journeys
async function getConversionJourneyAnalysis(redis, resetProgress = false) {
  console.log('📊 Analyzing conversions vs existing journeys...');
  
  // Get total conversions count
  const totalConversions = await getTotalConversionsCount(redis);
  
  if (resetProgress) {
    console.log('🔄 Reset requested - clearing all existing journeys');
    await clearAllExistingJourneys(redis);
    return {
      total_conversions: totalConversions,
      existing_journeys: 0,
      remaining_conversions: totalConversions,
      processed_order_ids: new Set()
    };
  }
  
  // Get existing journey order IDs
  const processedOrderIds = await getExistingJourneyOrderIds(redis);
  
  console.log(`📊 Found ${totalConversions} total conversions, ${processedOrderIds.size} existing journeys`);
  
  return {
    total_conversions: totalConversions,
    existing_journeys: processedOrderIds.size, // Only complete journeys
    remaining_conversions: totalConversions - processedOrderIds.size, // Includes broken journeys that need reprocessing
    processed_order_ids: processedOrderIds
  };
}

// FIXED: Get accurate total conversions count
async function getTotalConversionsCount(redis) {
  let totalCount = 0;
  let cursor = '0';
  const maxIterations = 50;
  let iterations = 0;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      totalCount += keys.length;
      iterations++;
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (error) {
    console.warn('⚠️ Error counting conversions:', error.message);
  }
  
  console.log(`📊 Total conversions found: ${totalCount}`);
  return totalCount;
}

// FIXED: Get existing journey order IDs for tracking progress - ONLY count journeys with touchpoints
async function getExistingJourneyOrderIds(redis) {
  const processedOrderIds = new Set();
  const brokenJourneyIds = new Set(); // Track broken journeys for reprocessing
  let cursor = '0';
  const maxIterations = 50;
  let iterations = 0;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      // Load journey data to get order IDs
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`, 1000);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              if (journey.conversion_order_id) {
                // CRITICAL FIX: Only count journeys with touchpoints as "processed"
                if (journey.total_touchpoints > 0) {
                  return { orderId: journey.conversion_order_id, status: 'complete' };
                } else {
                  // Mark broken journeys for potential reprocessing
                  return { orderId: journey.conversion_order_id, status: 'broken', key: key };
                }
              }
            }
          } catch (e) {
            // Skip invalid journeys
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        batchResults.filter(result => result !== null).forEach(result => {
          if (result.status === 'complete') {
            processedOrderIds.add(result.orderId);
          } else if (result.status === 'broken') {
            brokenJourneyIds.add(result.orderId);
          }
        });
      }
      
      iterations++;
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (error) {
    console.warn('⚠️ Error loading existing journey order IDs:', error.message);
  }
  
  console.log(`📊 Found ${processedOrderIds.size} complete journeys (with touchpoints)`);
  console.log(`📊 Found ${brokenJourneyIds.size} broken journeys (0 touchpoints) - these will be reprocessed`);
  return processedOrderIds;
}

// Clear all existing journeys for reset
async function clearAllExistingJourneys(redis) {
  let cursor = '0';
  let deletedCount = 0;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      // Delete journeys in batches
      for (const key of keys) {
        try {
          await redis(`del/${key}`);
          deletedCount++;
        } catch (e) {
          // Skip deletion errors
        }
      }
      
    } while (cursor !== '0' && deletedCount < 10000); // Safety limit
    
  } catch (error) {
    console.warn('⚠️ Error clearing existing journeys:', error.message);
  }
  
  console.log(`🗑️ Cleared ${deletedCount} existing journey records`);
}

// FIXED: Load remaining conversions in chronological order
async function loadRemainingConversions(redis, processedOrderIds, targetEmail = null, maxConversions = 100) {
  console.log('📊 Loading remaining conversions in chronological order...');
  
  const conversions = [];
  let cursor = '0';
  let keysScanned = 0;
  const maxIterations = 50;
  let iterations = 0;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Load conversion data
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const conversionData = await redis(`get/${key}`, 1500);
            if (conversionData?.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // FIXED: Check correct field names (customer_email AND email)
              const email = conversion.customer_email || conversion.email;
              const orderId = conversion.conversion_order_id || conversion.order_id;
              
              // Skip if already processed (only journeys with touchpoints are considered "processed")
              if (processedOrderIds.has(orderId)) {
                return null;
              }
              
              // Filter by target email if specified
              if (targetEmail && email !== targetEmail) {
                return null;
              }
              
              // Only process conversions with email and order_id
              if (email && orderId) {
                return {
                  ...conversion,
                  // Normalize field names
                  customer_email: email,
                  conversion_order_id: orderId,
                  timestamp: conversion.timestamp || conversion.conversion_timestamp,
                  _redis_key: key
                };
              }
            }
          } catch (parseError) {
            // Skip invalid data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validConversions = batchResults.filter(c => c !== null);
        conversions.push(...validConversions);
        
        // Stop if we've reached max conversions
        if (maxConversions && conversions.length >= maxConversions) {
          break;
        }
      }
      
      if (maxConversions && conversions.length >= maxConversions) {
        break;
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (error) {
    console.warn('⚠️ Error loading conversions:', error.message);
  }
  
  // FIXED: Sort chronologically (most recent first for faster processing of recent conversions)
  conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  const finalConversions = conversions.slice(0, maxConversions || 100);
  console.log(`✅ Loaded ${finalConversions.length} remaining conversions for processing`);
  
  if (finalConversions.length > 0) {
    console.log(`📅 Date range: ${finalConversions[finalConversions.length-1]?.timestamp} to ${finalConversions[0]?.timestamp}`);
  }
  
  return finalConversions;
}

// FIXED: Process batch with better error handling and attribution debugging
async function processBatchFixed(redis, conversions) {
  let attributionCalls = 0;
  let attributionSuccesses = 0;
  
  const batchPromises = conversions.map(async (conversion) => {
    try {
      attributionCalls++;
      const journey = await buildJourneyFromConversionFixed(redis, conversion);
      
      if (journey && journey.total_touchpoints > 0) {
        attributionSuccesses++;
        console.log(`✅ Attribution success for ${conversion.customer_email}: ${journey.total_touchpoints} touchpoints`);
      } else {
        console.log(`❌ Attribution failed for ${conversion.customer_email} (Order: ${conversion.conversion_order_id})`);
      }
      
      return journey;
    } catch (error) {
      console.warn('⚠️ Failed to build journey:', conversion.conversion_order_id, error.message);
      return null;
    }
  });
  
  const batchJourneys = await Promise.all(batchPromises);
  const validJourneys = batchJourneys.filter(journey => journey !== null);
  
  return {
    journeys: validJourneys,
    attribution_calls: attributionCalls,
    attribution_successes: attributionSuccesses
  };
}

// FIXED: Build journey with corrected field names and better debugging
async function buildJourneyFromConversionFixed(redis, conversion) {
  const orderId = conversion.conversion_order_id;
  if (!orderId) {
    console.warn('⚠️ No order ID found for conversion:', conversion);
    return null;
  }

  const journeyId = `journey_${orderId}_${Date.now()}`;
  
  // FIXED: Extract ALL available IP addresses with correct field names
  const allIPs = [];
  if (conversion.primary_ip && conversion.primary_ip !== 'unknown') {
    allIPs.push(conversion.primary_ip);
  }
  if (conversion.conversion_ip && conversion.conversion_ip !== 'unknown' && conversion.conversion_ip !== conversion.primary_ip) {
    allIPs.push(conversion.conversion_ip);
  }
  if (conversion.pageview_ip && conversion.pageview_ip !== 'unknown' && !allIPs.includes(conversion.pageview_ip)) {
    allIPs.push(conversion.pageview_ip);
  }
  // Also check IP field variants
  if (conversion.ip_address && conversion.ip_address !== 'unknown' && !allIPs.includes(conversion.ip_address)) {
    allIPs.push(conversion.ip_address);
  }
  if (conversion.IP && conversion.IP !== 'unknown' && !allIPs.includes(conversion.IP)) {
    allIPs.push(conversion.IP);
  }

  console.log(`🔍 Building journey for ${conversion.customer_email} (Order: ${orderId}) with ${allIPs.length} IPs:`, allIPs);

  let pageviews = [];
  let attributionMethod = 'conversion_only';
  let attributionScore = 0;

  try {
    // FIXED: Use enhanced attribution with proper debugging
    const attributionParams = {
      conversion_timestamp: conversion.timestamp,
      ips_to_check: allIPs,
      session_id: conversion.session_id,
      device_signature: conversion.device_signature || conversion.dsig || conversion.canvas_fingerprint,
      screen_value: conversion.screen_value || conversion.SVV,
      gpu_signature: conversion.gpu_signature || conversion.gsig,
      window_hours: 168 // 7 days
    };

    console.log(`🔍 Attribution params:`, {
      ips: allIPs.length,
      session: !!attributionParams.session_id,
      device: !!attributionParams.device_signature,
      window_hours: attributionParams.window_hours
    });

    const attributionResults = await performFixedAttribution(redis, attributionParams);
    
    if (attributionResults && attributionResults.length > 0) {
      pageviews = attributionResults.map(pv => ({
        timestamp: pv.timestamp,
        landing_page: pv.landing_page,
        source: pv.source,
        utm_source: pv.utm_source,
        utm_medium: pv.utm_medium,
        utm_campaign: pv.utm_campaign,
        utm_term: pv.utm_term,
        utm_content: pv.utm_content,
        referrer_url: pv.referrer_url,
        session_id: pv.session_id,
        confidence: pv.confidence,
        attribution_method: pv.attribution_method,
        matched_ip: pv.matched_ip,
        ip_source: pv.ip_source
      }));

      // Use highest confidence attribution method
      const highestConfidence = Math.max(...pageviews.map(pv => pv.confidence || 0));
      const primaryAttribution = pageviews.find(pv => pv.confidence === highestConfidence);
      
      attributionMethod = primaryAttribution?.attribution_method || 'ip_match';
      attributionScore = highestConfidence;
      
      console.log(`✅ Journey built: ${pageviews.length} pageviews, method: ${attributionMethod}, score: ${attributionScore}`);
    } else {
      console.log(`❌ No pageviews found for conversion ${orderId}`);
    }

  } catch (error) {
    console.warn('⚠️ Attribution failed:', orderId, error.message);
  }

  // Build journey object with correct field names
  const journey = {
    journey_id: journeyId,
    customer_email: conversion.customer_email,
    conversion_order_id: orderId,
    conversion_timestamp: conversion.timestamp,
    conversion_value: parseFloat(conversion.order_total || conversion.value || 0),
    total_touchpoints: pageviews.length,
    pageviews: pageviews,
    attribution_method: attributionMethod,
    attribution_score: attributionScore,
    
    // Dual-IP tracking
    dual_ip_scenario: allIPs.length > 1,
    ip_addresses_checked: allIPs,
    
    // Journey analysis
    first_click_source: pageviews.length > 0 ? pageviews[0].source : null,
    last_click_source: pageviews.length > 0 ? pageviews[pageviews.length - 1].source : null,
    journey_span_hours: pageviews.length > 0 ? 
      Math.round((new Date(conversion.timestamp).getTime() - new Date(pageviews[0].timestamp).getTime()) / (1000 * 60 * 60)) : 0,
    
    created_at: new Date().toISOString()
  };

  return journey;
}

// FIXED: Attribution logic with better debugging
async function performFixedAttribution(redis, params) {
  const { conversion_timestamp, ips_to_check, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  
  console.log(`🔍 Attribution window: ${new Date(windowStart).toISOString()} to ${new Date(conversionTime).toISOString()}`);
  
  let allMatches = [];
  
  try {
    // PRIORITY 1: Enhanced IP Index Search (this should work since query-pageviews-enhanced works)
    if (ips_to_check && ips_to_check.length > 0) {
      console.log(`🔍 Checking ${ips_to_check.length} IP addresses for pageviews...`);
      
      for (let i = 0; i < ips_to_check.length; i++) {
        const ip = ips_to_check[i];
        if (!ip || ip === 'unknown') continue;
        
        console.log(`   Checking IP ${i + 1}/${ips_to_check.length}: ${ip}`);
        
        const ipMatches = await searchByFixedIPIndexes(redis, ip, windowStart, conversionTime);
        
        if (ipMatches.length > 0) {
          console.log(`   ✅ Found ${ipMatches.length} pageviews for IP: ${ip}`);
          
          // Set confidence based on IP priority
          const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
          const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'pageview_ip';
          
          ipMatches.forEach(match => {
            match.attribution_method = `${ipType}_match`;
            match.confidence = Math.max(match.confidence || 0, confidence);
            match.ip_source = ipType;
            match.matched_ip = ip;
          });
          
          allMatches = allMatches.concat(ipMatches);
        } else {
          console.log(`   ❌ No pageviews found for IP: ${ip}`);
        }
      }
    }
    
    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
    
    console.log(`📊 Attribution result: ${uniqueMatches.length} unique pageviews found`);
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Fixed attribution error:', error.message);
    return [];
  }
}

// FIXED: IP index search with better error handling and debugging
async function searchByFixedIPIndexes(redis, ip, windowStart, windowEnd) {
  const matches = [];
  const encodedIP = ip.replace(/[:.]/g, '_');
  const ipIndexKey = `pageview_index_ip:${encodedIP}`;
  
  try {
    console.log(`    🔍 Looking up index key: ${ipIndexKey}`);
    
    const indexData = await redis(`get/${ipIndexKey}`);
    
    if (!indexData?.result) {
      console.log(`    ❌ No index found for IP: ${ip}`);
      return [];
    }
    
    const parsed = JSON.parse(decodeURIComponent(indexData.result));
    console.log(`    📊 Index contains ${parsed.pageviews?.length || 0} pageviews`);
    
    if (!parsed.pageviews || parsed.pageviews.length === 0) {
      console.log(`    ❌ Index exists but no pageviews in it`);
      return [];
    }
    
    // Filter pageviews within time window
    const windowPageviews = parsed.pageviews.filter(pv => {
      const pvTime = new Date(pv.timestamp).getTime();
      return pvTime >= windowStart && pvTime <= windowEnd;
    });
    
    console.log(`    📅 ${windowPageviews.length} pageviews within time window`);
    
    // Return all pageviews in time window with basic confidence
    for (const pv of windowPageviews) {
      matches.push({
        ...pv,
        matched_ip: ip,
        match_method: 'enhanced_ip_index',
        attribution_method: 'ip_index_match',
        confidence: 240,
        index_source: 'enhanced_ip_index'
      });
    }
    
  } catch (error) {
    console.warn(`⚠️ IP index search failed for ${ip}:`, error.message);
  }
  
  console.log(`    📊 Returning ${matches.length} matches for IP: ${ip}`);
  return matches;
}

// Remove duplicate matches
function removeDuplicateMatches(matches) {
  const seen = new Set();
  return matches.filter(match => {
    const key = `${match.timestamp}_${match.session_id || match.ip_address || 'unknown'}`;
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
  
  return async (command, timeoutMs = 5000) => {
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
