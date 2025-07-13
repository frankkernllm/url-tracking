// netlify/functions/build-customer-journeys.js
// STATELESS Customer Journey Builder - Direct Attribution Logic Embedded
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

  try {
    console.log('🚀 STATELESS CUSTOMER JOURNEY BUILDER: Starting with embedded attribution logic...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      journey_window_hours = 168, // 7-day journey lookback window
      batch_size = 20            // Process 20 conversions per batch
    } = body;
    
    console.log(`📊 Journey Parameters: ${journey_window_hours}h lookback window, batch size: ${batch_size}`);
    
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
    
    // Step 2: Filter out conversions that already have journeys (simple check)
    const conversionsNeedingJourneys = await filterConversionsNeedingJourneys(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    console.log(`📊 Journey Status: ${conversionsNeedingJourneys.length} need processing, ${allConversions.length - conversionsNeedingJourneys.length} already complete`);
    
    if (conversionsNeedingJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          build_complete: true,
          message: 'ALL CUSTOMER JOURNEYS COMPLETE! 🎉',
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
    
    console.log(`✅ Stateless processing complete: ${processingResults.journeys_created_this_run} journeys in ${totalTime}ms`);
    
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
          completion_percentage: completionPercentage,
          processing_time_ms: totalTime,
          embedded_attribution_calls: processingResults.attribution_calls_made,
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

// CHANGE 2: Load ALL conversions (no date filtering)
async function loadAllConversionsStateless(redis, maxTime) {
  console.log(`🔍 Loading ALL conversions from database (no date limits)...`);
  
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

// CHANGE 3: Simplified resume detection (just check if journey exists)
async function filterConversionsNeedingJourneys(redis, allConversions, maxTime) {
  console.log(`🔍 Checking ${allConversions.length} conversions for existing journeys (simple check)...`);
  
  const checkStartTime = Date.now();
  const conversionsNeedingJourneys = [];
  
  // Process conversions in batches
  const batchSize = 50;
  for (let i = 0; i < allConversions.length; i += batchSize) {
    if (Date.now() - checkStartTime > maxTime - 2000) {
      console.log('⏰ Time limit during journey existence check, stopping');
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
      console.log(`📊 Journey existence check progress: ${i + batchSize}/${allConversions.length} checked, ${conversionsNeedingJourneys.length} need journeys`);
    }
  }
  
  console.log(`✅ Journey existence check complete: ${conversionsNeedingJourneys.length} conversions need journey processing`);
  return conversionsNeedingJourneys;
}

// CHANGE 1 & 4: Process conversions with EMBEDDED attribution logic (no external API calls)
async function processConversionsWithEmbeddedAttribution(redis, conversionsToProcess, journeyWindowHours, batchSize, maxTime) {
  const processStartTime = Date.now();
  console.log(`🚀 Processing ${conversionsToProcess.length} conversions with EMBEDDED attribution logic...`);
  
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
    console.log(`🔗 Processing conversion batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${conversionsToProcess.length}`);
    
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
  
  console.log(`🏁 Processing summary: ${journeysCreated} journeys, ${attributionSuccessRate}% attribution success rate`);
  
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

// CHANGE 1: Process batch with EMBEDDED attribution logic (core enhancement)
async function processBatchWithEmbeddedAttribution(redis, conversions, journeyWindowHours) {
  const journeys = [];
  let attributionCalls = 0;
  let attributionSuccesses = 0;
  
  const batchPromises = conversions.map(async (conversion) => {
    try {
      attributionCalls++;
      
      // EMBEDDED ATTRIBUTION LOGIC - No external API calls!
      const attributionStartTime = Date.now();
      const journeyPageviews = await performEmbeddedAttribution(redis, {
        conversion_timestamp: conversion.timestamp,
        ips_to_check: conversion.ip_addresses,
        session_id: conversion.session_id,
        device_signature: conversion.device_signature,
        screen_value: conversion.screen_value,
        gpu_signature: conversion.gpu_signature,
        window_hours: journeyWindowHours
      });
      
      if (journeyPageviews && journeyPageviews.length > 0) {
        attributionSuccesses++;
        
        // Build complete customer journey from found pageviews
        const journey = buildJourneyFromPageviews(conversion, journeyPageviews);
        
        // Store journey record
        await storeCustomerJourney(redis, journey);
        
        return journey;
      } else {
        // Create conversion-only journey if no pageviews found
        const conversionOnlyJourney = createConversionOnlyJourney(conversion);
        await storeCustomerJourney(redis, conversionOnlyJourney);
        return conversionOnlyJourney;
      }
      
    } catch (journeyError) {
      console.warn(`⚠️ Error building journey for conversion ${conversion.order_id}:`, journeyError.message);
      // Create fallback journey
      const fallbackJourney = createConversionOnlyJourney(conversion);
      await storeCustomerJourney(redis, fallbackJourney);
      return fallbackJourney;
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

// CHANGE 1: EMBEDDED attribution logic (replaces external API call)
async function performEmbeddedAttribution(redis, params) {
  const { conversion_timestamp, ips_to_check, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  
  let allMatches = [];
  
  try {
    // PRIORITY 1: Enhanced IP Index Multi-Signal Search (from query-pageviews-enhanced.js logic)
    if (ips_to_check && ips_to_check.length > 0) {
      const ipMatches = await searchByEnhancedIPIndexes(redis, ips_to_check, session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime);
      if (ipMatches.length > 0) {
        allMatches = allMatches.concat(ipMatches);
        
        // If we found high-confidence matches, return immediately
        const highConfidenceMatches = ipMatches.filter(match => match.confidence >= 250);
        if (highConfidenceMatches.length > 0) {
          return highConfidenceMatches;
        }
      }
    }
    
    // FALLBACK METHODS: Only if no matches found in enhanced IP indexes
    if (allMatches.length === 0) {
      
      // PRIORITY 2: Session ID Match (direct lookup)
      if (session_id) {
        const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
        if (sessionMatches.length > 0) {
          sessionMatches.forEach(match => {
            match.attribution_method = 'session_id_match_direct';
            match.confidence = 300;
          });
          allMatches = allMatches.concat(sessionMatches);
        }
      }
      
      // PRIORITY 3: Device Signature Match (direct lookup)
      if (device_signature && allMatches.length === 0) {
        const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
        if (deviceMatches.length > 0) {
          deviceMatches.forEach(match => {
            match.attribution_method = 'device_signature_match_direct';
            match.confidence = 260;
          });
          allMatches = allMatches.concat(deviceMatches);
        }
      }
      
      // PRIORITY 4: Basic IP Address Matches (direct lookup)
      if (ips_to_check && ips_to_check.length > 0 && allMatches.length === 0) {
        for (let i = 0; i < ips_to_check.length; i++) {
          const ip = ips_to_check[i];
          if (!ip || ip === 'unknown') continue;
          
          const ipMatches = await searchByIpAddress(redis, ip, windowStart, conversionTime);
          if (ipMatches.length > 0) {
            const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
            const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
            
            ipMatches.forEach(match => {
              match.attribution_method = `${ipType}_match_direct`;
              match.confidence = confidence;
            });
            
            allMatches = allMatches.concat(ipMatches);
            break; // Stop at first IP match
          }
        }
      }
    }
    
    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
    
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Embedded attribution error:', error.message);
    return [];
  }
}

// Enhanced IP index search (embedded from query-pageviews-enhanced.js)
async function searchByEnhancedIPIndexes(redis, ipsToCheck, sessionId, deviceSignature, screenValue, gpuSignature, windowStart, windowEnd) {
  const matches = [];
  
  for (const ip of ipsToCheck) {
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        
        if (!parsed.multi_signal_ready) continue;
        
        // Filter pageviews within time window
        const windowPageviews = parsed.pageviews.filter(pv => {
          const pvTime = new Date(pv.timestamp);
          return pvTime >= windowStart && pvTime <= windowEnd;
        });
        
        // Multi-signal matching within IP index
        for (const pv of windowPageviews) {
          let confidence = 240; // Base IP match confidence
          let attributionMethod = 'ip_index_match';
          
          // PRIORITY 1: Session ID match (highest confidence)
          if (sessionId && pv.session_id === sessionId) {
            confidence = 295;
            attributionMethod = 'session_id_match_ip_index';
          }
          // PRIORITY 2: Device signature match
          else if (deviceSignature && pv.canvas_fingerprint === deviceSignature) {
            confidence = 255;
            attributionMethod = 'device_signature_match_ip_index';
          }
          // PRIORITY 3: Screen signature match
          else if (screenValue && pv.screen_resolution && hashString(pv.screen_resolution) === screenValue) {
            confidence = 195;
            attributionMethod = 'screen_signature_match_ip_index';
          }
          // PRIORITY 4: GPU signature match
          else if (gpuSignature && pv.webgl_fingerprint && hashString(pv.webgl_fingerprint) === gpuSignature) {
            confidence = 175;
            attributionMethod = 'webgl_signature_match_ip_index';
          }
          
          matches.push({
            ...pv,
            matched_ip: ip,
            match_method: 'enhanced_ip_index_multi_signal',
            attribution_method: attributionMethod,
            confidence: confidence,
            index_source: 'enhanced_ip_index'
          });
        }
      }
      
    } catch (error) {
      console.warn(`⚠️ Enhanced IP index search failed for ${ip}:`, error.message);
    }
  }
  
  return matches;
}

// Session ID search (embedded logic)
async function searchBySessionId(redis, sessionId, windowStart, conversionTime) {
  try {
    const sessionKey = `attribution_session_${sessionId}`;
    const sessionResult = await redis(`get/${sessionKey}`);
    
    if (sessionResult.result) {
      const attributionResult = await redis(`get/${sessionResult.result}`);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    return [];
  }
}

// Device signature search (embedded logic)
async function searchByDeviceSignature(redis, deviceSig, windowStart, conversionTime) {
  try {
    const deviceKey = `attribution_fp_${deviceSig}`;
    const deviceResult = await redis(`get/${deviceKey}`);
    
    if (deviceResult.result) {
      const attributionResult = await redis(`get/${deviceResult.result}`);
      if (attributionResult.result) {
        const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
        const pageviewTime = new Date(pageview.timestamp).getTime();
        
        if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
          return [pageview];
        }
      }
    }
    
    return [];
  } catch (error) {
    return [];
  }
}

// IP address search (embedded logic)
async function searchByIpAddress(redis, ip, windowStart, conversionTime) {
  try {
    const encodedIp = ip.replace(/:/g, '_');
    const ipKey = `attribution_ip_${encodedIp}`;
    const ipResult = await redis(`get/${ipKey}`);
    
    if (ipResult.result) {
      const attributionKeys = Array.isArray(ipResult.result) ? ipResult.result : [ipResult.result];
      const matches = [];
      
      for (const key of attributionKeys) {
        try {
          const attributionResult = await redis(`get/${key}`);
          if (attributionResult.result) {
            const pageview = JSON.parse(decodeURIComponent(attributionResult.result));
            const pageviewTime = new Date(pageview.timestamp).getTime();
            
            if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
              matches.push(pageview);
            }
          }
        } catch (e) {
          // Skip invalid data
        }
      }
      
      return matches;
    }
    
    return [];
  } catch (error) {
    return [];
  }
}

// Helper function for hashing (same as store-attribution.js)
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

// Build journey from pageviews and conversion
function buildJourneyFromPageviews(conversion, pageviews) {
  // Sort pageviews by timestamp (earliest first)
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
    
    created_at: new Date().toISOString(),
    reconstruction_method: 'stateless_embedded_attribution'
  };
}

// Create journey for conversions without pageviews
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
    reconstruction_method: 'stateless_conversion_only_fallback'
  };
}

// Store individual customer journey
async function storeCustomerJourney(redis, journey) {
  const journeyKey = `customer_journey:${journey.journey_id}`;
  await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
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
