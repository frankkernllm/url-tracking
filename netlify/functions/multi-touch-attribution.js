// Multi-Touch Attribution Engine - Phase 1: Single Conversion Attribution
// Path: netlify/functions/multi-touch-attribution.js
// Purpose: Reconstruct complete customer journeys by matching conversions with pageview data

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    // Parse request body
    const requestData = JSON.parse(event.body || '{}');
    const { email, timestamp, conversion_index } = requestData;
    
    if (!email) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing required field: email' 
        })
      };
    }
    
    console.log(`🎯 Starting multi-touch attribution for: ${email}${timestamp ? ` at ${timestamp}` : ' (most recent conversion)'}`);
    
    // Step 1: Get conversion data from conversion indexes
    console.log('📊 Step 1: Looking up conversion data...');
    const conversionData = await getConversionData(redis, email, timestamp, conversion_index);
    
    if (!conversionData) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ 
          error: 'Conversion not found in conversion indexes',
          email: email,
          timestamp: timestamp || 'auto-select most recent',
          conversion_index: conversion_index || 0
        })
      };
    }
    
    console.log('✅ Conversion found:', {
      email: conversionData.email,
      order_total: conversionData.order_total,
      conversion_ip: conversionData.conversion_ip,
      primary_ip: conversionData.primary_ip,
      ssid: conversionData.ssid
    });
    
    // Step 2: Multi-criteria pageview lookup
    console.log('🔍 Step 2: Performing multi-criteria pageview lookup...');
    const attributionResult = await performMultiTouchAttribution(redis, conversionData);
    
    // Step 3: Store attribution result permanently
    console.log('💾 Step 3: Storing attribution result permanently...');
    const storageResult = await storeAttributionResult(redis, attributionResult);
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ Multi-touch attribution completed in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        attribution_result: attributionResult,
        conversion_selection: {
          email: email,
          timestamp: timestamp || 'auto-selected',
          conversion_index: timestamp ? 'exact_match' : (conversion_index || 0),
          selection_method: timestamp ? 'timestamp_match' : 'index_selection'
        },
        storage: {
          stored_permanently: storageResult.success,
          storage_key: storageResult.key,
          storage_verified: storageResult.verified
        },
        processing_time_ms: processingTime
      })
    };
    
  } catch (error) {
    console.error('❌ Multi-touch attribution failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Multi-touch attribution failed', 
        message: error.message 
      })
    };
  }
};

// Get conversion data from conversion indexes
async function getConversionData(redis, email, timestamp, conversionIndex = 0) {
  try {
    // Look up conversion by email index
    const emailIndexKey = `conversion_index_v1_email:${encodeURIComponent(email)}`;
    console.log(`🔍 Looking up conversion index: ${emailIndexKey}`);
    
    const indexResult = await redis(`get/${emailIndexKey}`, 3000);
    
    if (!indexResult?.result) {
      console.log('❌ No conversion index found for email');
      return null;
    }
    
    const conversionIndexData = JSON.parse(decodeURIComponent(indexResult.result));
    console.log(`📊 Found conversion index with ${conversionIndexData.conversion_count} conversions`);
    
    // If timestamp provided, find exact match
    if (timestamp) {
      console.log(`🎯 Looking for specific timestamp: ${timestamp}`);
      const targetConversion = conversionIndexData.conversions.find(conv => 
        conv.timestamp === timestamp
      );
      
      if (!targetConversion) {
        console.log('❌ Specific conversion timestamp not found in index');
        console.log('📋 Available conversions:', conversionIndexData.conversions.map(c => ({
          timestamp: c.timestamp,
          order_total: c.order_total
        })));
        return null;
      }
      
      console.log('✅ Target conversion found by timestamp');
      return targetConversion;
    }
    
    // If no timestamp, use conversion index (default 0 = most recent)
    if (conversionIndexData.conversions.length === 0) {
      console.log('❌ No conversions found in index');
      return null;
    }
    
    if (conversionIndex >= conversionIndexData.conversions.length) {
      console.log(`❌ Conversion index ${conversionIndex} out of range (0-${conversionIndexData.conversions.length - 1})`);
      return null;
    }
    
    const selectedConversion = conversionIndexData.conversions[conversionIndex];
    console.log(`✅ Selected conversion #${conversionIndex} (${conversionIndex === 0 ? 'most recent' : 'historical'}): ${selectedConversion.timestamp}`);
    console.log('📋 Available conversions:', conversionIndexData.conversions.map((c, i) => ({
      index: i,
      timestamp: c.timestamp,
      order_total: c.order_total,
      selected: i === conversionIndex
    })));
    
    return selectedConversion;
    
  } catch (error) {
    console.log('❌ Error looking up conversion data:', error.message);
    return null;
  }
}

// Perform multi-touch attribution analysis
async function performMultiTouchAttribution(redis, conversionData) {
  console.log('🎯 Starting multi-touch attribution analysis...');
  
  const conversionTime = new Date(conversionData.timestamp);
  const allPageviews = [];
  const seenPageviews = new Set();
  const attributionMethods = [];
  
  // Attribution Query 1: Session ID lookup (if available)
  if (conversionData.ssid) {
    console.log(`🔗 Query 1: Session ID lookup for: ${conversionData.ssid}`);
    
    try {
      const sessionKey = `attribution_index_v1_session:${conversionData.ssid}`;
      const sessionResult = await redis(`get/${sessionKey}`, 3000);
      
      if (sessionResult?.result) {
        const sessionIndex = JSON.parse(decodeURIComponent(sessionResult.result));
        console.log(`✅ Session index found: ${sessionIndex.pageview_count} pageviews`);
        
        attributionMethods.push('session_match');
        addPageviewsToJourney(sessionIndex.pageviews, 'session_match', allPageviews, seenPageviews, conversionTime);
      } else {
        console.log('⚠️ No session index found');
      }
    } catch (sessionError) {
      console.log('⚠️ Session lookup error:', sessionError.message);
    }
  } else {
    console.log('⚠️ No session ID available for lookup');
  }
  
  // Attribution Query 2: Primary IP lookup
  if (conversionData.primary_ip) {
    console.log(`🌐 Query 2: Primary IP lookup for: ${conversionData.primary_ip}`);
    
    try {
      const encodedPrimaryIP = encodeIPForKey(conversionData.primary_ip);
      const primaryIPKey = `attribution_index_v1_ip:${encodedPrimaryIP}`;
      const primaryIPResult = await redis(`get/${primaryIPKey}`, 3000);
      
      if (primaryIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(primaryIPResult.result));
        console.log(`✅ Primary IP index found: ${ipIndex.pageview_count} pageviews`);
        
        if (!attributionMethods.includes('primary_ip_match')) {
          attributionMethods.push('primary_ip_match');
        }
        addPageviewsToJourney(ipIndex.pageviews, 'primary_ip_match', allPageviews, seenPageviews, conversionTime);
      } else {
        console.log('⚠️ No primary IP index found');
      }
    } catch (ipError) {
      console.log('⚠️ Primary IP lookup error:', ipError.message);
    }
  }
  
  // Attribution Query 3: Conversion IP lookup (if different from primary)
  if (conversionData.conversion_ip && conversionData.conversion_ip !== conversionData.primary_ip) {
    console.log(`🌐 Query 3: Conversion IP lookup for: ${conversionData.conversion_ip}`);
    
    try {
      const encodedConversionIP = encodeIPForKey(conversionData.conversion_ip);
      const conversionIPKey = `attribution_index_v1_ip:${encodedConversionIP}`;
      const conversionIPResult = await redis(`get/${conversionIPKey}`, 3000);
      
      if (conversionIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(conversionIPResult.result));
        console.log(`✅ Conversion IP index found: ${ipIndex.pageview_count} pageviews`);
        
        if (!attributionMethods.includes('conversion_ip_match')) {
          attributionMethods.push('conversion_ip_match');
        }
        addPageviewsToJourney(ipIndex.pageviews, 'conversion_ip_match', allPageviews, seenPageviews, conversionTime);
      } else {
        console.log('⚠️ No conversion IP index found');
      }
    } catch (ipError) {
      console.log('⚠️ Conversion IP lookup error:', ipError.message);
    }
  }
  
  // Sort customer journey chronologically (oldest first)
  const customerJourney = allPageviews.sort((a, b) => 
    new Date(a.timestamp) - new Date(b.timestamp)
  );
  
  // Add touchpoint numbers
  customerJourney.forEach((pageview, index) => {
    pageview.touchpoint_number = index + 1;
  });
  
  // Calculate attribution summary
  const attributionSummary = calculateAttributionSummary(customerJourney, conversionData, attributionMethods);
  
  console.log(`🎯 Attribution analysis complete:`);
  console.log(`   📊 Total touchpoints: ${customerJourney.length}`);
  console.log(`   🔍 Attribution methods: ${attributionMethods.join(', ')}`);
  console.log(`   📅 Journey duration: ${attributionSummary.journey_duration_days} days`);
  
  return {
    conversion: {
      email: conversionData.email,
      timestamp: conversionData.timestamp,
      order_total: conversionData.order_total,
      landing_page: conversionData.landing_page,
      conversion_ip: conversionData.conversion_ip,
      primary_ip: conversionData.primary_ip,
      ssid: conversionData.ssid
    },
    attribution_summary: attributionSummary,
    customer_journey: customerJourney
  };
}

// Add pageviews to journey with deduplication
function addPageviewsToJourney(pageviews, attributionMethod, allPageviews, seenPageviews, conversionTime) {
  let addedCount = 0;
  let filteredCount = 0;
  
  pageviews.forEach(pageview => {
    const pageviewTime = new Date(pageview.timestamp);
    
    // Only include pageviews that occurred BEFORE the conversion
    if (pageviewTime >= conversionTime) {
      filteredCount++;
      return;
    }
    
    // Deduplicate using session_id + timestamp combination
    const dedupeKey = `${pageview.session_id}_${pageview.timestamp}`;
    
    if (!seenPageviews.has(dedupeKey)) {
      seenPageviews.add(dedupeKey);
      allPageviews.push({
        ...pageview,
        attribution_method: attributionMethod
      });
      addedCount++;
    }
  });
  
  console.log(`   📝 Added ${addedCount} pageviews via ${attributionMethod}, filtered ${filteredCount} future pageviews`);
}

// Calculate attribution summary
function calculateAttributionSummary(customerJourney, conversionData, attributionMethods) {
  if (customerJourney.length === 0) {
    return {
      total_touchpoints: 0,
      journey_duration_days: 0,
      attribution_methods_used: attributionMethods,
      unique_sessions: 0,
      unique_sources: [],
      unique_campaigns: [],
      first_touch: null,
      last_touch: null,
      attribution_confidence: calculateAttributionConfidence(customerJourney, conversionData, attributionMethods)
    };
  }
  
  const firstTouch = customerJourney[0];
  const lastTouch = customerJourney[customerJourney.length - 1];
  
  const journeyStart = new Date(firstTouch.timestamp);
  const journeyEnd = new Date(lastTouch.timestamp);
  const journeyDurationMs = journeyEnd - journeyStart;
  const journeyDurationDays = Math.ceil(journeyDurationMs / (1000 * 60 * 60 * 24));
  
  const uniqueSessions = [...new Set(customerJourney.map(pv => pv.session_id).filter(Boolean))];
  const uniqueSources = [...new Set(customerJourney.map(pv => pv.source).filter(Boolean))];
  const uniqueCampaigns = [...new Set(customerJourney.map(pv => pv.utm_campaign).filter(Boolean))];
  
  return {
    total_touchpoints: customerJourney.length,
    journey_duration_days: journeyDurationDays,
    attribution_methods_used: attributionMethods,
    unique_sessions: uniqueSessions.length,
    unique_sources: uniqueSources,
    unique_campaigns: uniqueCampaigns,
    first_touch: {
      timestamp: firstTouch.timestamp,
      landing_page: firstTouch.landing_page,
      source: firstTouch.source,
      utm_campaign: firstTouch.utm_campaign,
      attribution_method: firstTouch.attribution_method
    },
    last_touch: {
      timestamp: lastTouch.timestamp,
      landing_page: lastTouch.landing_page,
      source: lastTouch.source,
      utm_campaign: lastTouch.utm_campaign,
      attribution_method: lastTouch.attribution_method
    },
    attribution_confidence: calculateAttributionConfidence(customerJourney, conversionData, attributionMethods)
  };
}

// Calculate attribution confidence score
function calculateAttributionConfidence(customerJourney, conversionData, attributionMethods) {
  let score = 0;
  const factors = {};
  
  // Session ID available (high confidence)
  if (conversionData.ssid && attributionMethods.includes('session_match')) {
    score += 40;
    factors.session_id_available = true;
  } else {
    factors.session_id_available = false;
  }
  
  // Cross-device detection
  const crossDeviceDetected = conversionData.conversion_ip !== conversionData.primary_ip;
  factors.cross_device_detected = crossDeviceDetected;
  if (crossDeviceDetected) {
    score += 20; // Cross-device attribution is valuable
  }
  
  // Journey completeness
  if (customerJourney.length >= 5) {
    score += 20;
    factors.journey_completeness = 'high';
  } else if (customerJourney.length >= 2) {
    score += 10;
    factors.journey_completeness = 'medium';
  } else {
    factors.journey_completeness = 'low';
  }
  
  // Multiple attribution methods
  if (attributionMethods.length >= 2) {
    score += 15;
    factors.multiple_attribution_methods = true;
  } else {
    factors.multiple_attribution_methods = false;
  }
  
  // Recent journey (higher confidence for recent data)
  if (customerJourney.length > 0) {
    const lastTouchTime = new Date(customerJourney[customerJourney.length - 1].timestamp);
    const conversionTime = new Date(conversionData.timestamp);
    const timeDiffHours = (conversionTime - lastTouchTime) / (1000 * 60 * 60);
    
    if (timeDiffHours <= 24) {
      score += 5;
      factors.temporal_accuracy = 'excellent';
    } else if (timeDiffHours <= 168) { // 1 week
      score += 3;
      factors.temporal_accuracy = 'good';
    } else {
      factors.temporal_accuracy = 'fair';
    }
  }
  
  return {
    score: Math.min(score, 100), // Cap at 100
    factors: factors
  };
}

// Store attribution result permanently in Redis
async function storeAttributionResult(redis, attributionResult) {
  const storageKey = `multi_touch_attribution:${attributionResult.conversion.email}:${attributionResult.conversion.timestamp}`;
  
  // Add storage metadata
  const enrichedResult = {
    ...attributionResult,
    storage_metadata: {
      stored_at: new Date().toISOString(),
      storage_key: storageKey,
      attribution_version: 'v1',
      storage_permanent: true
    }
  };
  
  try {
    console.log('💾 Storing permanent attribution result:', storageKey);
    
    // PERMANENT storage - NO TTL specified (following track.js pattern)
    const setResult = await redis(`set/${storageKey}/${encodeURIComponent(JSON.stringify(enrichedResult))}`);
    console.log('📋 Redis set returned:', JSON.stringify(setResult));
    
    // Verify storage (following track.js verification pattern)
    console.log('🔍 Verifying attribution write with get command...');
    const verifyResult = await redis(`get/${storageKey}`, 3000);
    console.log('📋 Redis get verification:', verifyResult ? 'DATA FOUND ✅' : 'DATA NOT FOUND ❌');
    
    if (verifyResult && verifyResult.result) {
      console.log('✅ VERIFIED: Attribution result successfully written and readable (PERMANENT STORAGE)');
      return { 
        success: true, 
        key: storageKey, 
        verified: true 
      };
    } else {
      console.log('❌ CRITICAL: Attribution result not found after set command - Redis write failed silently');
      return { 
        success: false, 
        error: 'Attribution result not found after write attempt - silent Redis failure', 
        verified: false 
      };
    }
    
  } catch (error) {
    console.log('❌ Attribution storage operation failed:', error.message);
    return { 
      success: false, 
      error: error.message, 
      verified: false 
    };
  }
}

// Utility function for encoding IPs (same as build-attribution-indexes.js)
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

// Initialize Redis helper (same pattern as other endpoints)
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
