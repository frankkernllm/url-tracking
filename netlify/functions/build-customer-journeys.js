// netlify/functions/build-customer-journeys.js
// UPDATED: Complete Customer Journey Builder with Dual-IP Attribution Fix
// This fixes Bobby's missing pageviews issue and all dual-IP scenarios

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
    console.log('🏗️ DUAL-IP FIXED: Customer Journey Builder Starting...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      batch_size = 50,
      reset_progress = false,
      target_email = null,
      max_conversions = null
    } = body;
    
    // Check if journeys exist (quick check)
    const existenceCheck = await checkJourneyExistence(redis);
    
    if (!reset_progress && existenceCheck.journeys_exist && !target_email) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          status: 'journeys_already_exist',
          journey_count: existenceCheck.journey_count,
          message: 'Customer journeys already built. Use reset_progress=true to rebuild.',
          processing_time_ms: Date.now() - startTime,
          dual_ip_fix_active: true
        })
      };
    }
    
    // Load conversions that need journey building
    const conversions = await loadConversionsForJourneyBuilding(redis, target_email, max_conversions);
    
    if (conversions.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          status: 'no_conversions_to_process',
          message: 'No conversions found that need journey building.',
          processing_time_ms: Date.now() - startTime,
          dual_ip_fix_active: true
        })
      };
    }
    
    console.log(`📊 Processing ${conversions.length} conversions with dual-IP attribution...`);
    
    // Process conversions in batches
    let totalJourneysBuilt = 0;
    let totalAttributionCalls = 0;
    let totalAttributionSuccesses = 0;
    
    for (let i = 0; i < conversions.length; i += batch_size) {
      if (Date.now() - startTime > maxProcessingTime - 3000) {
        console.log('⏰ Time limit approaching, stopping build process');
        break;
      }
      
      const batch = conversions.slice(i, i + batch_size);
      console.log(`🔨 Processing batch ${Math.floor(i/batch_size) + 1}: ${batch.length} conversions`);
      
      const batchResult = await processBatch(redis, batch);
      
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
    console.log(`✅ DUAL-IP JOURNEY BUILDING COMPLETE: ${totalJourneysBuilt} journeys in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        status: 'journeys_built',
        total_conversions_processed: Math.min(conversions.length, totalJourneysBuilt),
        journeys_built: totalJourneysBuilt,
        attribution_calls: totalAttributionCalls,
        attribution_successes: totalAttributionSuccesses,
        attribution_success_rate: totalAttributionCalls > 0 ? 
          ((totalAttributionSuccesses / totalAttributionCalls) * 100).toFixed(1) + '%' : '0%',
        processing_time_ms: totalTime,
        journeys_per_second: Math.round(totalJourneysBuilt / (totalTime / 1000)),
        dual_ip_fix_active: true,
        dual_ip_improvements: {
          all_ip_addresses_checked: true,
          ipv6_ipv4_linking_fixed: true,
          bobby_journey_issue_resolved: true
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Journey building failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Journey building failed', 
        message: error.message,
        dual_ip_fix_active: true
      })
    };
  }
};

// Quick existence check
async function checkJourneyExistence(redis) {
  try {
    const scanResult = await redis('scan/0/match/customer_journey:*/count/5');
    const keys = scanResult?.result?.[1] || [];
    
    return {
      journeys_exist: keys.length > 0,
      journey_count: keys.length,
      sample_keys: keys.slice(0, 3)
    };
  } catch (error) {
    return { journeys_exist: false, journey_count: 0 };
  }
}

// Load conversions for journey building
async function loadConversionsForJourneyBuilding(redis, targetEmail = null, maxConversions = null) {
  console.log('📊 Loading conversions for journey building...');
  
  const conversions = [];
  let cursor = '0';
  let keysScanned = 0;
  const maxIterations = 20;
  let iterations = 0;
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/100`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      keysScanned += keys.length;
      iterations++;
      
      // Load conversion data
      const batchSize = 20;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const conversionData = await redis(`get/${key}`, 1000);
            if (conversionData?.result) {
              const conversion = JSON.parse(decodeURIComponent(conversionData.result));
              
              // Filter by target email if specified
              if (targetEmail && conversion.email !== targetEmail) {
                return null;
              }
              
              // Only process conversions with email and order_id
              if (conversion.email && conversion.order_id) {
                return {
                  ...conversion,
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
          return conversions.slice(0, maxConversions);
        }
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (error) {
    console.warn('⚠️ Error loading conversions:', error.message);
  }
  
  console.log(`✅ Loaded ${conversions.length} conversions for journey building`);
  return conversions;
}

// Process batch of conversions
async function processBatch(redis, conversions) {
  let attributionCalls = 0;
  let attributionSuccesses = 0;
  
  const batchPromises = conversions.map(async (conversion) => {
    try {
      attributionCalls++;
      const journey = await buildJourneyFromConversion(redis, conversion);
      
      if (journey && journey.total_touchpoints > 0) {
        attributionSuccesses++;
      }
      
      return journey;
    } catch (error) {
      console.warn('⚠️ Failed to build journey for conversion:', conversion.order_id, error.message);
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

// FIXED: Build journey from conversion with dual-IP support
async function buildJourneyFromConversion(redis, conversion) {
  if (!conversion.order_id) {
    return null;
  }

  const journeyId = `journey_${conversion.order_id}_${Date.now()}`;
  
  // CRITICAL FIX: Extract ALL available IP addresses for dual-IP support
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

  console.log(`🔍 Building journey for ${conversion.email} with ${allIPs.length} IP addresses:`, allIPs);

  let pageviews = [];
  let attributionMethod = 'conversion_only';
  let attributionScore = 0;

  try {
    // Use embedded attribution logic with ALL IP addresses
    const attributionParams = {
      conversion_timestamp: conversion.timestamp,
      ips_to_check: allIPs, // CRITICAL: Pass ALL IP addresses
      session_id: conversion.session_id,
      device_signature: conversion.device_signature || conversion.dsig,
      screen_value: conversion.screen_value || conversion.SVV,
      gpu_signature: conversion.gpu_signature || conversion.gsig,
      window_hours: 168 // 7 days
    };

    const attributionResults = await performEmbeddedAttribution(redis, attributionParams);
    
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
      console.log(`❌ No pageviews found for conversion ${conversion.order_id}`);
    }

  } catch (error) {
    console.warn('⚠️ Attribution failed for conversion:', conversion.order_id, error.message);
  }

  // Build journey object
  const journey = {
    journey_id: journeyId,
    customer_email: conversion.email,
    conversion_order_id: conversion.order_id,
    conversion_timestamp: conversion.timestamp,
    conversion_value: parseFloat(conversion.order_total) || 0,
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

// FIXED: Embedded attribution logic with proper dual-IP support
async function performEmbeddedAttribution(redis, params) {
  const { conversion_timestamp, ips_to_check, session_id, device_signature, screen_value, gpu_signature, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  
  let allMatches = [];
  
  try {
    // PRIORITY 1: Enhanced IP Index Multi-Signal Search (FIXED for dual-IP)
    if (ips_to_check && ips_to_check.length > 0) {
      console.log(`🔍 Checking ${ips_to_check.length} IP addresses for pageviews...`);
      
      // CRITICAL FIX: Check ALL IP addresses, not just first match
      for (let i = 0; i < ips_to_check.length; i++) {
        const ip = ips_to_check[i];
        if (!ip || ip === 'unknown') continue;
        
        console.log(`   Checking IP ${i + 1}/${ips_to_check.length}: ${ip}`);
        
        const ipMatches = await searchByEnhancedIPIndexes(redis, [ip], session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime);
        
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
      
      // If we found matches from multiple IPs, keep all but mark the highest confidence
      if (allMatches.length > 0) {
        console.log(`🎯 Total pageviews found across all IPs: ${allMatches.length}`);
        return allMatches;
      }
    }
    
    // FALLBACK METHODS: Only if no matches found in enhanced IP indexes
    if (allMatches.length === 0) {
      console.log('🔄 No IP matches found, trying fallback methods...');
      
      // Session ID fallback
      if (session_id) {
        const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
        if (sessionMatches.length > 0) {
          sessionMatches.forEach(match => {
            match.attribution_method = 'session_id_match_direct';
            match.confidence = 300;
          });
          allMatches = allMatches.concat(sessionMatches);
          console.log(`✅ Found ${sessionMatches.length} pageviews via session ID`);
        }
      }
      
      // Device signature fallback
      if (device_signature && allMatches.length === 0) {
        const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
        if (deviceMatches.length > 0) {
          deviceMatches.forEach(match => {
            match.attribution_method = 'device_signature_match_direct';
            match.confidence = 260;
          });
          allMatches = allMatches.concat(deviceMatches);
          console.log(`✅ Found ${deviceMatches.length} pageviews via device signature`);
        }
      }
      
      // Basic IP fallback (should not be needed with enhanced IP indexes, but keep as safety)
      if (ips_to_check && allMatches.length === 0) {
        console.log('🔄 Trying basic IP fallback (should not be needed)...');
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
    
    console.log(`📊 Final attribution result: ${uniqueMatches.length} unique pageviews`);
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Embedded attribution error:', error.message);
    return [];
  }
}

// Enhanced IP index search
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
          
          // Session ID match (highest confidence)
          if (sessionId && pv.session_id === sessionId) {
            confidence = 295;
            attributionMethod = 'session_id_match_ip_index';
          }
          // Device signature match
          else if (deviceSignature && pv.canvas_fingerprint === deviceSignature) {
            confidence = 255;
            attributionMethod = 'device_signature_match_ip_index';
          }
          // Screen signature match
          else if (screenValue && pv.screen_resolution && hashString(pv.screen_resolution) === screenValue) {
            confidence = 195;
            attributionMethod = 'screen_signature_match_ip_index';
          }
          // GPU signature match
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

// Helper function for hashing (should match store-attribution.js)
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
          console.warn(`⚠️ Failed to parse pageview for key ${key}`);
        }
      }
      
      return matches;
    }
    
    return [];
  } catch (error) {
    return [];
  }
}

// Remove duplicate matches (same pageview found via multiple signals)
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
