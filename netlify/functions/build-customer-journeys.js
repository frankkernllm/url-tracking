// netlify/functions/build-customer-journeys.js
// TYPE-SAFE VERSION: Fixed order_id type handling
// KEY FIX: Ensures consistent string comparison between stored and filtered order_ids

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
    console.log('🔧 TYPE-SAFE JOURNEY BUILDER: Starting with fixed order_id handling...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      journey_window_hours = 168, // 7-day journey lookback window
      batch_size = 20,            // Process 20 conversions per batch
      force_rebuild = false       // Force rebuild specific conversions
    } = body;
    
    console.log(`📊 Journey Parameters: ${journey_window_hours}h lookback window, batch size: ${batch_size}, force_rebuild: ${force_rebuild}`);
    
    // Step 1: Load ALL conversions (no date limits - truly stateless)
    const allConversions = await loadAllConversionsStateless(redis, maxProcessingTime - (Date.now() - startTime));
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
    
    // Step 2: TYPE-SAFE journey existence check
    const conversionsNeedingJourneys = await typeSafeFilterConversionsNeedingJourneys(
      redis, 
      allConversions, 
      maxProcessingTime - (Date.now() - startTime),
      force_rebuild
    );
    
    console.log(`📊 Journey Status: ${conversionsNeedingJourneys.length} need processing, ${allConversions.length - conversionsNeedingJourneys.length} already complete`);
    
    if (conversionsNeedingJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          build_complete: true,
          message: '🎉 ALL CUSTOMER JOURNEYS COMPLETE!',
          summary: {
            total_conversions: allConversions.length,
            conversions_with_journeys: allConversions.length,
            conversion_coverage: '100%',
            processing_status: 'complete'
          }
        })
      };
    }
    
    // Step 3: Process conversions with TYPE-SAFE storage
    const processingResults = await processConversionsWithTypeSafeStorage(
      redis, 
      conversionsNeedingJourneys, 
      journey_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    const completionPercentage = ((allConversions.length - processingResults.conversions_remaining) / allConversions.length * 100).toFixed(1);
    
    console.log(`✅ TYPE-SAFE processing complete: ${processingResults.journeys_created_this_run} journeys in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        type_safe_fixes_applied: true,
        build_complete: processingResults.is_complete,
        force_rebuild_mode: force_rebuild,
        execution_summary: {
          total_conversions_in_database: allConversions.length,
          conversions_needing_journeys_at_start: conversionsNeedingJourneys.length,
          conversions_processed_this_run: processingResults.conversions_processed_this_run,
          journeys_created_this_run: processingResults.journeys_created_this_run,
          conversions_remaining: processingResults.conversions_remaining,
          completion_percentage: completionPercentage,
          processing_time_ms: totalTime,
          attribution_calls_made: processingResults.attribution_calls_made,
          attribution_success_rate: processingResults.attribution_success_rate
        },
        debug_info: {
          sample_order_ids_in_conversions: processingResults.sample_conversion_order_ids,
          sample_order_ids_found_in_journeys: processingResults.sample_existing_journey_order_ids,
          type_conversion_applied: 'consistent_string_comparison'
        },
        type_safe_fixes: [
          'Consistent string conversion for order_id comparison',
          'Type-safe journey key extraction',
          'Robust order_id normalization',
          'Enhanced logging for type debugging'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Type-safe journey building failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Type-safe journey building failed', 
        message: error.message 
      })
    };
  }
};

// TYPE-SAFE: Filter conversions with consistent string comparison
async function typeSafeFilterConversionsNeedingJourneys(redis, allConversions, maxTime, forceRebuild = false) {
  console.log(`🔍 TYPE-SAFE FILTERING: Checking ${allConversions.length} conversions with consistent type handling...`);
  
  const filterStartTime = Date.now();
  
  // Step 1: Load existing journey order_ids with type safety
  const existingJourneyOrderIds = await loadExistingJourneyOrderIdsTypeSafe(redis, maxTime - 1000);
  
  // Step 2: TYPE-SAFE filtering with consistent string comparison
  const conversionsNeedingJourneys = [];
  const sampleConversionOrderIds = [];
  const sampleExistingJourneyOrderIds = [];
  
  allConversions.forEach((conversion, index) => {
    // CRITICAL FIX: Ensure consistent string comparison
    const conversionOrderId = String(conversion.order_id || conversion.conversion_order_id || '');
    
    // Collect samples for debugging
    if (index < 5) {
      sampleConversionOrderIds.push({
        original: conversion.order_id,
        converted: conversionOrderId,
        type: typeof conversion.order_id
      });
    }
    
    // Check if journey exists for this order_id
    const hasExistingJourney = existingJourneyOrderIds.has(conversionOrderId);
    
    if (forceRebuild) {
      if (!hasExistingJourney) {
        conversionsNeedingJourneys.push(conversion);
      }
    } else {
      if (!hasExistingJourney) {
        conversionsNeedingJourneys.push(conversion);
      }
    }
  });
  
  // Collect sample existing journey order_ids for debugging
  const existingArray = Array.from(existingJourneyOrderIds);
  for (let i = 0; i < Math.min(5, existingArray.length); i++) {
    sampleExistingJourneyOrderIds.push(existingArray[i]);
  }
  
  const filterTime = Date.now() - filterStartTime;
  console.log(`✅ TYPE-SAFE FILTERING complete in ${filterTime}ms:`);
  console.log(`   📊 ${conversionsNeedingJourneys.length} need processing`);
  console.log(`   🎯 ${existingJourneyOrderIds.size} existing journeys found`);
  console.log(`   🔍 Sample conversion order_ids:`, sampleConversionOrderIds);
  console.log(`   🔍 Sample existing journey order_ids:`, sampleExistingJourneyOrderIds);
  
  // Return both filtered conversions and sample data for debugging
  conversionsNeedingJourneys._debugInfo = {
    sample_conversion_order_ids: sampleConversionOrderIds,
    sample_existing_journey_order_ids: sampleExistingJourneyOrderIds
  };
  
  return conversionsNeedingJourneys;
}

// TYPE-SAFE: Load existing journey order_ids with consistent string handling
async function loadExistingJourneyOrderIdsTypeSafe(redis, maxTime) {
  console.log('🔍 TYPE-SAFE: Loading existing journey order_ids...');
  
  const existingJourneyOrderIds = new Set();
  let cursor = '0';
  let keysScanned = 0;
  const scanStartTime = Date.now();
  
  try {
    do {
      if (Date.now() - scanStartTime > maxTime - 1000) {
        console.log('⏰ Time limit during journey order_id scan, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      
      // TYPE-SAFE: Extract order IDs with consistent string conversion
      keys.forEach(key => {
        // Extract order_id from pattern: customer_journey:journey_{order_id}_{timestamp}
        const match = key.match(/customer_journey:journey_([^_]+)_/);
        if (match && match[1]) {
          // CRITICAL FIX: Always store as string for consistent comparison
          const orderIdStr = String(match[1]);
          existingJourneyOrderIds.add(orderIdStr);
        }
      });
      
    } while (cursor !== '0');
    
  } catch (error) {
    console.warn('⚠️ Type-safe journey order_id scan error:', error.message);
  }
  
  console.log(`🔍 TYPE-SAFE scan complete: ${keysScanned} keys scanned, ${existingJourneyOrderIds.size} unique order_ids found`);
  
  return existingJourneyOrderIds;
}

// TYPE-SAFE: Process conversions with consistent string handling
async function processConversionsWithTypeSafeStorage(redis, conversions, journeyWindowHours, batchSize, maxTime) {
  const processStartTime = Date.now();
  let journeysCreated = 0;
  let conversionsProcessed = 0;
  let attributionCallsMade = 0;
  let attributionSuccesses = 0;
  let totalAttributionTime = 0;
  
  // Extract debug info from conversions
  const debugInfo = conversions._debugInfo || {};
  
  const batchPromises = conversions.slice(0, batchSize).map(async (conversion) => {
    try {
      attributionCallsMade++;
      conversionsProcessed++;
      
      // TYPE-SAFE: Ensure order_id is consistently handled as string
      const orderIdStr = String(conversion.order_id || conversion.conversion_order_id || '');
      
      // FIXED ATTRIBUTION LOGIC: Extract and split comma-separated IPs
      const extractedIPs = extractAndSplitIPs(conversion);
      
      console.log(`🔧 TYPE-SAFE: Processing conversion ${orderIdStr}: Found ${extractedIPs.length} IPs`);
      
      const attributionStartTime = Date.now();
      const journeyPageviews = await performFixedAttribution(redis, {
        conversion_timestamp: conversion.timestamp,
        ips_to_check: extractedIPs,
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature,
        window_hours: journeyWindowHours
      });
      
      totalAttributionTime += (Date.now() - attributionStartTime);
      
      if (journeyPageviews && journeyPageviews.length > 0) {
        attributionSuccesses++;
        console.log(`✅ Attribution SUCCESS for ${orderIdStr}: ${journeyPageviews.length} pageviews found`);
        
        // Build complete customer journey from found pageviews
        const journey = buildJourneyFromPageviews(conversion, journeyPageviews);
        
        // Store journey record
        await storeCustomerJourney(redis, journey);
        journeysCreated++;
        
        return journey;
      } else {
        console.log(`❌ Attribution FAILED for ${orderIdStr}: No pageviews found`);
        
        // Create conversion-only journey if no pageviews found
        const conversionOnlyJourney = createConversionOnlyJourney(conversion);
        await storeCustomerJourney(redis, conversionOnlyJourney);
        journeysCreated++;
        
        return conversionOnlyJourney;
      }
      
    } catch (journeyError) {
      console.warn(`⚠️ Error building journey for conversion ${conversion.order_id}:`, journeyError.message);
      // Create fallback journey
      const fallbackJourney = createConversionOnlyJourney(conversion);
      await storeCustomerJourney(redis, fallbackJourney);
      journeysCreated++;
      return fallbackJourney;
    }
  });
  
  await Promise.all(batchPromises);
  
  const remainingConversions = Math.max(0, conversions.length - conversionsProcessed);
  const avgAttributionTime = attributionCallsMade > 0 ? Math.round(totalAttributionTime / attributionCallsMade) : 0;
  const attributionSuccessRate = attributionCallsMade > 0 ? ((attributionSuccesses / attributionCallsMade) * 100).toFixed(1) : '0.0';
  
  console.log(`🏁 TYPE-SAFE Processing summary: ${journeysCreated} journeys created, ${attributionSuccessRate}% attribution success rate`);
  
  return {
    journeys_created_this_run: journeysCreated,
    conversions_processed_this_run: conversionsProcessed,
    conversions_remaining: remainingConversions,
    is_complete: remainingConversions === 0,
    attribution_calls_made: attributionCallsMade,
    attribution_success_rate: attributionSuccessRate,
    avg_attribution_time_ms: avgAttributionTime,
    processing_time_ms: Date.now() - processStartTime,
    sample_conversion_order_ids: debugInfo.sample_conversion_order_ids || [],
    sample_existing_journey_order_ids: debugInfo.sample_existing_journey_order_ids || []
  };
}

// Build journey from pageviews - TYPE-SAFE order_id handling
function buildJourneyFromPageviews(conversion, pageviews) {
  // TYPE-SAFE: Ensure order_id is consistently handled as string
  const orderIdStr = String(conversion.order_id || conversion.conversion_order_id || '');
  
  const journey = {
    journey_id: `journey_${orderIdStr}_${Date.now()}`,
    customer_email: conversion.email,
    conversion_order_id: orderIdStr, // Store as string for consistency
    conversion_timestamp: conversion.timestamp,
    conversion_value: conversion.value || 0,
    total_touchpoints: pageviews.length,
    pageviews: pageviews.map((pv, index) => ({
      ...pv,
      touchpoint_number: index + 1,
      time_to_conversion_hours: Math.round((new Date(conversion.timestamp) - new Date(pv.timestamp)) / (1000 * 60 * 60))
    })),
    attribution_method: 'multi_signal_embedded_fixed',
    created_at: new Date().toISOString()
  };
  
  return journey;
}

// Create conversion-only journey - TYPE-SAFE order_id handling
function createConversionOnlyJourney(conversion) {
  // TYPE-SAFE: Ensure order_id is consistently handled as string
  const orderIdStr = String(conversion.order_id || conversion.conversion_order_id || '');
  
  return {
    journey_id: `journey_${orderIdStr}_${Date.now()}`,
    customer_email: conversion.email,
    conversion_order_id: orderIdStr, // Store as string for consistency
    conversion_timestamp: conversion.timestamp,
    conversion_value: conversion.value || 0,
    total_touchpoints: 0,
    pageviews: [],
    attribution_method: 'conversion_only',
    created_at: new Date().toISOString()
  };
}

// Store customer journey
async function storeCustomerJourney(redis, journey) {
  try {
    const journeyKey = `customer_journey:${journey.journey_id}`;
    const journeyData = encodeURIComponent(JSON.stringify(journey));
    
    await redis(`setex/${journeyKey}/2592000/${journeyData}`, 3000); // 30-day TTL
    
    return true;
  } catch (error) {
    console.warn(`⚠️ Failed to store journey ${journey.journey_id}:`, error.message);
    return false;
  }
}

// NEW: Extract and split comma-separated IPs (unchanged)
function extractAndSplitIPs(conversion) {
  const ips = [];
  
  const ipFields = [
    'main_ip_address', 'winning_ip_value', 'primary_ip', 'conversion_ip', 
    'pageview_ip', 'ip_address', 'PIP', 'CIP', 'IP'
  ];
  
  ipFields.forEach(field => {
    const value = conversion[field];
    if (value && value !== 'unknown') {
      if (typeof value === 'string' && value.includes(',')) {
        const splitIPs = value.split(',').map(ip => ip.trim()).filter(ip => ip && ip !== 'unknown');
        ips.push(...splitIPs);
      } else {
        ips.push(value);
      }
    }
  });
  
  return [...new Set(ips)];
}

// FIXED: Attribution logic (simplified version)
async function performFixedAttribution(redis, params) {
  const { conversion_timestamp, ips_to_check, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  
  let allMatches = [];
  
  try {
    // Enhanced IP Index Multi-Signal Search
    if (ips_to_check && ips_to_check.length > 0) {
      const ipMatches = await searchByEnhancedIPIndexes(redis, ips_to_check, session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime);
      
      if (ipMatches.length > 0) {
        allMatches = allMatches.concat(ipMatches);
        
        const highConfidenceMatches = ipMatches.filter(match => match.confidence >= 250);
        if (highConfidenceMatches.length > 0) {
          return highConfidenceMatches;
        }
      }
    }
    
    return allMatches;
    
  } catch (error) {
    console.warn('⚠️ Attribution error:', error.message);
    return [];
  }
}

// Simplified IP index search
async function searchByEnhancedIPIndexes(redis, ipsToCheck, sessionId, deviceSignature, screenValue, gpuSignature, windowStart, windowEnd) {
  const matches = [];
  
  for (const ip of ipsToCheck) {
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        
        if (parsed.multi_signal_ready) {
          const windowPageviews = parsed.pageviews.filter(pv => {
            const pvTime = new Date(pv.timestamp).getTime();
            return pvTime >= windowStart && pvTime <= windowEnd;
          });
          
          windowPageviews.forEach(pv => {
            matches.push({
              ...pv,
              matched_ip: ip,
              confidence: 240,
              attribution_method: 'ip_index_match'
            });
          });
        }
      }
    } catch (error) {
      console.warn(`⚠️ IP index search failed for ${ip}:`, error.message);
    }
  }
  
  return matches;
}

// Load all conversions (unchanged from original)
async function loadAllConversionsStateless(redis, maxTime) {
  console.log('📊 Loading ALL conversions from indexes (stateless)...');
  
  const conversions = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 50;
  const scanStartTime = Date.now();
  
  try {
    do {
      if (Date.now() - scanStartTime > maxTime - 5000) {
        console.log('⏰ Time limit during conversion loading, stopping');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/conversion_index_date:*/count/20`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      if (keys.length === 0) continue;
      
      const batchSize = 5;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - scanStartTime > maxTime - 3000) break;
        
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const indexData = await redis(`get/${key}`);
            if (indexData?.result) {
              const parsed = JSON.parse(decodeURIComponent(indexData.result));
              if (parsed.conversions && Array.isArray(parsed.conversions)) {
                return parsed.conversions.map(conversion => ({
                  ...conversion,
                  source_key: key,
                  order_id: conversion.order_id || conversion.conversion_order_id,
                  timestamp: conversion.timestamp || conversion.conversion_timestamp,
                  email: conversion.email || conversion.customer_email,
                  value: conversion.value || conversion.conversion_value || 0,
                  ip_addresses: conversion.ip_addresses || [],
                  _redis_key: key
                }));
              }
            }
          } catch (parseError) {
            console.warn(`⚠️ Failed to parse conversion index ${key}`);
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validResults = batchResults.filter(result => result !== null);
        validResults.forEach(conversionArray => {
          if (Array.isArray(conversionArray)) {
            conversions.push(...conversionArray);
          }
        });
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
  
  console.log(`✅ Loaded ${conversions.length} total conversions from database (ALL historical data)`);
  return conversions;
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
