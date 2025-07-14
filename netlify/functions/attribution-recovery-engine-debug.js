// attribution-recovery-engine-debug.js
// DEBUG VERSION: Enhanced logging to diagnose data loading issues

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
    console.log('🐛 DEBUG ATTRIBUTION RECOVERY: Starting with enhanced logging...');
    const startTime = Date.now();
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      recovery_window_days = 40,         // Extended default
      extended_window_hours = 72,
      batch_size = 50,
      force_reprocess = false,
      debug_mode = true
    } = body;
    
    console.log(`🐛 DEBUG Parameters: ${recovery_window_days} day range, ${extended_window_hours}h attribution window`);
    
    // STEP 1: Debug conversion index loading
    console.log('🐛 STEP 1: DEBUG - Loading conversion indexes...');
    const conversionIndexes = await debugLoadConversionIndexes(redis, recovery_window_days);
    
    console.log('🐛 DEBUG - Conversion indexes loaded:');
    console.log(`   📊 Total conversions: ${conversionIndexes.totalConversions}`);
    console.log(`   📅 Date indexes: ${conversionIndexes.dateKeys.length}`);
    console.log(`   📅 Date range: ${conversionIndexes.dateKeys[0]} to ${conversionIndexes.dateKeys[conversionIndexes.dateKeys.length - 1]}`);
    
    if (conversionIndexes.totalConversions === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          debug_mode: true,
          issue_identified: 'No conversions found in date indexes',
          debug_info: {
            date_keys_checked: conversionIndexes.dateKeys,
            conversion_count: conversionIndexes.totalConversions,
            date_range_days: recovery_window_days
          },
          recommendations: [
            'Increase recovery_window_days parameter',
            'Check if conversion_index_date:* keys exist',
            'Verify date format matches'
          ]
        })
      };
    }
    
    // STEP 2: Debug sample conversion data structure
    console.log('🐛 STEP 2: DEBUG - Analyzing conversion data structure...');
    const sampleConversions = conversionIndexes.conversions.slice(0, 3);
    
    for (let i = 0; i < sampleConversions.length; i++) {
      const conversion = sampleConversions[i];
      console.log(`🐛 Sample conversion ${i + 1}:`, {
        order_id: conversion.order_id,
        conversion_order_id: conversion.conversion_order_id,
        timestamp: conversion.timestamp,
        email: conversion.email,
        primary_ip: conversion.primary_ip,
        conversion_ip: conversion.conversion_ip,
        pageview_ip: conversion.pageview_ip,
        ip_address: conversion.ip_address,
        available_fields: Object.keys(conversion)
      });
      
      const extractedIPs = debugExtractIPsFromConversion(conversion);
      console.log(`🐛 Extracted IPs for sample ${i + 1}:`, extractedIPs);
    }
    
    // STEP 3: Debug journey loading
    console.log('🐛 STEP 3: DEBUG - Loading conversion-only journeys...');
    const conversionOnlyJourneys = await debugLoadConversionOnlyJourneys(redis, force_reprocess);
    
    console.log(`🐛 DEBUG - Journey analysis:`);
    console.log(`   🎯 Conversion-only journeys: ${conversionOnlyJourneys.length}`);
    
    if (conversionOnlyJourneys.length > 0) {
      const sampleJourney = conversionOnlyJourneys[0];
      console.log(`🐛 Sample journey:`, {
        journey_id: sampleJourney.journey_id,
        conversion_order_id: sampleJourney.conversion_order_id,
        customer_email: sampleJourney.customer_email,
        conversion_timestamp: sampleJourney.conversion_timestamp
      });
    }
    
    // STEP 4: Debug matching process
    console.log('🐛 STEP 4: DEBUG - Testing journey-to-conversion matching...');
    const conversionsByOrderId = new Map();
    conversionIndexes.conversions.forEach(conversion => {
      const orderId = conversion.order_id || conversion.conversion_order_id;
      if (orderId) {
        conversionsByOrderId.set(String(orderId), conversion);
      }
    });
    
    console.log(`🐛 DEBUG - Conversion lookup map: ${conversionsByOrderId.size} conversions indexed by order_id`);
    
    // Test first few journeys
    let matchedJourneys = 0;
    let journeysWithMatchableIPs = 0;
    
    for (let i = 0; i < Math.min(5, conversionOnlyJourneys.length); i++) {
      const journey = conversionOnlyJourneys[i];
      const conversion = conversionsByOrderId.get(String(journey.conversion_order_id));
      
      console.log(`🐛 Testing journey ${i + 1}:`, {
        journey_order_id: journey.conversion_order_id,
        conversion_found: !!conversion,
        conversion_order_id: conversion?.order_id || conversion?.conversion_order_id
      });
      
      if (conversion) {
        matchedJourneys++;
        const extractedIPs = debugExtractIPsFromConversion(conversion);
        console.log(`🐛 Journey ${i + 1} IPs:`, extractedIPs);
        
        if (extractedIPs.length > 0) {
          journeysWithMatchableIPs++;
        }
      }
    }
    
    console.log(`🐛 DEBUG - Matching results from sample:`);
    console.log(`   ✅ Matched journeys: ${matchedJourneys}/5`);
    console.log(`   ✅ Journeys with IPs: ${journeysWithMatchableIPs}/5`);
    
    const totalTime = Date.now() - startTime;
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        debug_mode: true,
        debug_results: {
          conversion_only_journeys_found: conversionOnlyJourneys.length,
          conversions_in_indexes: conversionIndexes.totalConversions,
          date_indexes_loaded: conversionIndexes.dateKeys.length,
          date_range: {
            start: conversionIndexes.dateKeys[0],
            end: conversionIndexes.dateKeys[conversionIndexes.dateKeys.length - 1],
            window_days: recovery_window_days
          },
          sample_matching: {
            journeys_tested: Math.min(5, conversionOnlyJourneys.length),
            matched_journeys: matchedJourneys,
            journeys_with_ips: journeysWithMatchableIPs
          },
          processing_time_ms: totalTime
        },
        sample_data: {
          sample_conversions: sampleConversions.map(c => ({
            order_id: c.order_id,
            extracted_ips: debugExtractIPsFromConversion(c)
          })),
          sample_journey: conversionOnlyJourneys.length > 0 ? {
            journey_id: conversionOnlyJourneys[0].journey_id,
            conversion_order_id: conversionOnlyJourneys[0].conversion_order_id
          } : null
        },
        next_steps: [
          matchedJourneys === 0 ? 'Issue: No journey-to-conversion matching' : 'Journey matching working',
          journeysWithMatchableIPs === 0 ? 'Issue: No IPs extracted from conversions' : 'IP extraction working',
          conversionIndexes.totalConversions === 0 ? 'Issue: No conversion data loaded' : 'Conversion data loaded successfully'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Debug attribution recovery failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        debug_mode: true,
        error: 'Debug attribution recovery failed', 
        message: error.message,
        stack: error.stack
      })
    };
  }
};

// DEBUG: Load conversion indexes with enhanced logging
async function debugLoadConversionIndexes(redis, recoveryWindowDays) {
  console.log(`🐛 DEBUG: Loading conversion indexes for ${recoveryWindowDays} days...`);
  
  const conversions = [];
  const dateKeys = [];
  
  // Generate date keys for the recovery window
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - recoveryWindowDays);
  
  const datesToCheck = [];
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    const dateKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    datesToCheck.push(dateKey);
  }
  
  console.log(`🐛 DEBUG: Generated ${datesToCheck.length} date keys to check:`);
  console.log(`🐛 First 5 dates: ${datesToCheck.slice(0, 5).join(', ')}`);
  console.log(`🐛 Last 5 dates: ${datesToCheck.slice(-5).join(', ')}`);
  
  // Load conversion indexes
  for (const dateKey of datesToCheck) {
    try {
      const indexKey = `conversion_index_date:${dateKey}`;
      const indexData = await redis(`get/${indexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        dateKeys.push(dateKey);
        
        console.log(`🐛 DEBUG: Found index for ${dateKey}: ${parsed.conversion_count} conversions`);
        
        if (parsed.conversions && Array.isArray(parsed.conversions)) {
          const dateConversions = parsed.conversions.map(conversion => ({
            ...conversion,
            date_key: dateKey
          }));
          conversions.push(...dateConversions);
        }
      } else {
        console.log(`🐛 DEBUG: No index found for ${dateKey}`);
      }
    } catch (error) {
      console.log(`🐛 DEBUG: Error loading index for ${dateKey}:`, error.message);
    }
  }
  
  console.log(`🐛 DEBUG: Loaded ${conversions.length} conversions from ${dateKeys.length} date indexes`);
  
  return {
    conversions,
    dateKeys,
    totalConversions: conversions.length
  };
}

// DEBUG: Extract IPs with enhanced logging
function debugExtractIPsFromConversion(conversion) {
  const ips = [];
  const debugInfo = {};
  
  // Check all possible IP fields
  const ipFields = [
    'primary_ip', 'conversion_ip', 'pageview_ip', 'ip_address',
    'PIP', 'CIP', 'IP', 'main_ip_address'
  ];
  
  ipFields.forEach(field => {
    const value = conversion[field];
    debugInfo[field] = value;
    if (value && value !== 'unknown') {
      ips.push(value);
    }
  });
  
  const uniqueIPs = [...new Set(ips)];
  
  console.log(`🐛 DEBUG IP extraction:`, {
    order_id: conversion.order_id,
    available_ip_fields: debugInfo,
    extracted_ips: uniqueIPs
  });
  
  return uniqueIPs;
}

// DEBUG: Load journeys with enhanced logging
async function debugLoadConversionOnlyJourneys(redis, forceReprocess) {
  console.log('🐛 DEBUG: Loading conversion-only journeys...');
  
  const conversionOnlyJourneys = [];
  let cursor = '0';
  let totalJourneys = 0;
  
  do {
    const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/100`);
    
    if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
      break;
    }
    
    cursor = scanResult.result[0];
    const keys = scanResult.result[1] || [];
    
    for (const key of keys) {
      try {
        const journeyData = await redis(`get/${key}`);
        if (journeyData?.result) {
          const journey = JSON.parse(decodeURIComponent(journeyData.result));
          totalJourneys++;
          
          // Check if conversion-only
          const isConversionOnly = journey.total_touchpoints === 1 || 
                                 journey.reconstruction_method?.includes('conversion_only') ||
                                 (journey.touchpoints && journey.touchpoints.every(tp => tp.is_conversion || tp.type === 'conversion'));
          
          if (isConversionOnly) {
            const needsRecovery = forceReprocess || !journey.recovery_attempted;
            
            if (needsRecovery) {
              conversionOnlyJourneys.push({
                journey_id: journey.journey_id,
                journey_key: key,
                customer_email: journey.customer_email,
                conversion_order_id: journey.conversion_order_id,
                conversion_timestamp: journey.conversion_timestamp,
                conversion_value: journey.conversion_value,
                current_touchpoints: journey.total_touchpoints,
                recovery_attempted: journey.recovery_attempted || false
              });
            }
          }
        }
      } catch (parseError) {
        console.log(`🐛 DEBUG: Error parsing journey ${key}:`, parseError.message);
      }
    }
    
    if (conversionOnlyJourneys.length >= 10) break; // Limit for debug
    
  } while (cursor !== '0');
  
  console.log(`🐛 DEBUG: Found ${conversionOnlyJourneys.length} conversion-only journeys out of ${totalJourneys} total`);
  
  return conversionOnlyJourneys;
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
