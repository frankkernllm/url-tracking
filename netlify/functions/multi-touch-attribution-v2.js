// Multi-Touch Attribution Engine V2 - Enhanced Data Sources
// Path: netlify/functions/multi-touch-attribution-v2.js
// Purpose: Reconstruct complete customer journeys using V2 enhanced data with verification recovery
//
// V2 ENHANCEMENTS:
// - Uses V2 enhanced extraction + verification recovery data
// - 5 attribution methods (vs 3 in V1): session, primary_ip, conversion_ip, landing_page, source
// - Enhanced conversion lookup with V2 email validation and date fallback
// - Landing page and source attribution analysis
// - Verification data quality scoring
// - Target IP detection and cross-device attribution
//
// DEBUG PARAMETERS:
// - force_debug: true = Enable detailed console logging for troubleshooting
// - skip_existing_check: true = Process even if attribution already exists
// 
// Example debug usage:
// {
//   "email": "user@example.com", 
//   "timestamp": "2025-07-25T23:06:41.043Z",
//   "force_debug": true,
//   "skip_existing_check": true
// }

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
    const { email, timestamp, conversion_index, force_debug = false, skip_existing_check = false } = requestData;
    
    if (!email) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ 
          error: 'Missing required field: email',
          debug_tip: 'Add force_debug: true and skip_existing_check: true for debugging existing attributions'
        })
      };
    }
    
    console.log(`🚀 Starting V2 multi-touch attribution for: ${email}${timestamp ? ` at ${timestamp}` : ' (most recent conversion)'}`);
    if (force_debug) console.log('🔍 V2 DEBUG MODE ENABLED');
    if (skip_existing_check) console.log('🔍 V2 SKIP EXISTING CHECK ENABLED');
    
    // Step 1: Get conversion data from V2 conversion indexes
    console.log('📊 Step 1: Looking up conversion data from V2 indexes...');
    const conversionData = await getV2ConversionData(redis, email, timestamp, conversion_index);
    
    if (!conversionData) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ 
          error: 'Conversion not found in V2 conversion indexes',
          email: email,
          timestamp: timestamp || 'auto-select most recent',
          conversion_index: conversion_index || 0,
          note: 'Ensure extract-conversion-data-v2.js has completed successfully'
        })
      };
    }
    
    console.log('✅ V2 Conversion found:', {
      email: conversionData.email,
      order_total: conversionData.order_total,
      conversion_ip: conversionData.conversion_ip,
      primary_ip: conversionData.primary_ip,
      ssid: conversionData.ssid,
      landing_page: conversionData.landing_page
    });
    
    // Step 2: Enhanced multi-criteria pageview lookup using V2 indexes
    console.log('🔍 Step 2: Performing V2 enhanced multi-criteria pageview lookup...');
    
    // OPTIONAL: Skip existing check for debugging
    if (!skip_existing_check) {
      // Check if V2 attribution already exists
      const existingKey = `multi_touch_attribution_v2:${conversionData.email}:${conversionData.timestamp}`;
      const existingResult = await redis(`get/${existingKey}`, 1000);
      
      if (existingResult?.result) {
        console.log(`⚠️ V2 Attribution already exists for ${conversionData.email} at ${conversionData.timestamp}`);
        
        if (!force_debug) {
          return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
              success: true,
              message: 'V2 Attribution already exists',
              existing_attribution: JSON.parse(decodeURIComponent(existingResult.result)),
              note: 'Use skip_existing_check: true to force reprocessing or force_debug: true to debug existing attribution'
            })
          };
        } else {
          console.log('🔍 V2 FORCE_DEBUG enabled - will debug existing attribution');
        }
      }
    } else {
      console.log('🔍 V2 SKIP_EXISTING_CHECK enabled - will process even if attribution exists');
    }
    
    const attributionResult = await performV2MultiTouchAttribution(redis, conversionData, force_debug);
    
    // Step 3: Store V2 attribution result permanently
    console.log('💾 Step 3: Storing V2 attribution result permanently...');
    let storageResult = { success: true, key: 'debug_mode_no_storage', verified: false };
    
    if (!force_debug || skip_existing_check) {
      storageResult = await storeV2AttributionResult(redis, attributionResult);
    } else {
      console.log('🔍 V2 DEBUG MODE: Skipping storage to avoid overwriting existing attribution');
    }
    
    const processingTime = Date.now() - startTime;
    console.log(`✅ V2 Multi-touch attribution completed in ${processingTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        attribution_result: attributionResult,
        v2_enhancements: {
          enhanced_extraction_data: true,
          verification_recovery_data: true,
          landing_page_attribution: true,
          source_attribution: true,
          target_ip_detection: attributionResult.attribution_summary.target_ip_detected || false,
          data_sources: attributionResult.attribution_summary.data_sources_used || []
        },
        conversion_selection: {
          email: email,
          timestamp: timestamp || 'auto-selected',
          conversion_index: timestamp ? 'exact_match' : (conversion_index || 0),
          selection_method: timestamp ? 'timestamp_match' : 'index_selection',
          debug_mode: !!force_debug,
          skipped_existing_check: !!skip_existing_check
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
    console.error('❌ V2 Multi-touch attribution failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'V2 Multi-touch attribution failed', 
        message: error.message 
      })
    };
  }
};

// Get conversion data from V2 conversion indexes
async function getV2ConversionData(redis, email, timestamp, conversionIndex = 0) {
  try {
    // Primary: Look up conversion by V2 email index (enhanced email validation)
    const emailIndexKey = `conversion_index_v2_email:${encodeURIComponent(email)}`;
    console.log(`🔍 V2 Looking up conversion index: ${emailIndexKey}`);
    
    const indexResult = await redis(`get/${emailIndexKey}`, 3000);
    
    if (!indexResult?.result) {
      console.log('❌ No V2 conversion index found for email, trying date-based fallback...');
      
      // NEW: V2 date-based fallback if timestamp provided
      if (timestamp) {
        return await getV2ConversionByDate(redis, email, timestamp);
      }
      
      return null;
    }
    
    const conversionIndexData = JSON.parse(decodeURIComponent(indexResult.result));
    console.log(`📊 Found V2 conversion index with ${conversionIndexData.conversion_count} conversions`);
    
    // If timestamp provided, find exact match
    if (timestamp) {
      console.log(`🎯 Looking for specific timestamp: ${timestamp}`);
      const targetConversion = conversionIndexData.conversions.find(conv => 
        conv.timestamp === timestamp
      );
      
      if (!targetConversion) {
        console.log('❌ Specific conversion timestamp not found in V2 index');
        console.log('📋 Available V2 conversions:', conversionIndexData.conversions.map(c => ({
          timestamp: c.timestamp,
          order_total: c.order_total,
          landing_page: c.landing_page
        })));
        return null;
      }
      
      console.log('✅ Target conversion found by timestamp in V2 data');
      return targetConversion;
    }
    
    // If no timestamp, use conversion index (default 0 = most recent)
    if (conversionIndexData.conversions.length === 0) {
      console.log('❌ No conversions found in V2 index');
      return null;
    }
    
    if (conversionIndex >= conversionIndexData.conversions.length) {
      console.log(`❌ Conversion index ${conversionIndex} out of range (0-${conversionIndexData.conversions.length - 1})`);
      return null;
    }
    
    const selectedConversion = conversionIndexData.conversions[conversionIndex];
    console.log(`✅ Selected V2 conversion #${conversionIndex} (${conversionIndex === 0 ? 'most recent' : 'historical'}): ${selectedConversion.timestamp}`);
    console.log('📋 Available V2 conversions:', conversionIndexData.conversions.map((c, i) => ({
      index: i,
      timestamp: c.timestamp,
      order_total: c.order_total,
      landing_page: c.landing_page,
      selected: i === conversionIndex
    })));
    
    return selectedConversion;
    
  } catch (error) {
    console.log('❌ Error looking up V2 conversion data:', error.message);
    return null;
  }
}

// NEW: V2 date-based conversion lookup
async function getV2ConversionByDate(redis, email, timestamp) {
  try {
    const date = new Date(timestamp).toISOString().split('T')[0]; // YYYY-MM-DD
    const dateIndexKey = `conversion_index_v2_date:${date}`;
    console.log(`🗓️ V2 Date fallback lookup: ${dateIndexKey}`);
    
    const dateResult = await redis(`get/${dateIndexKey}`, 3000);
    
    if (!dateResult?.result) {
      console.log('❌ No V2 date index found');
      return null;
    }
    
    const dateIndexData = JSON.parse(decodeURIComponent(dateResult.result));
    console.log(`📊 Found V2 date index with ${dateIndexData.conversion_count} conversions`);
    
    // Find conversion by email and timestamp in date index
    const targetConversion = dateIndexData.conversions.find(conv => 
      conv.email === email && conv.timestamp === timestamp
    );
    
    if (targetConversion) {
      console.log('✅ Conversion found via V2 date index fallback');
      return targetConversion;
    } else {
      console.log('❌ Conversion not found in V2 date index');
      return null;
    }
    
  } catch (error) {
    console.log('❌ Error in V2 date-based lookup:', error.message);
    return null;
  }
}

// Perform V2 enhanced multi-touch attribution analysis (5 methods)
async function performV2MultiTouchAttribution(redis, conversionData, forceDebug = false) {
  console.log('🚀 Starting V2 enhanced multi-touch attribution analysis...');
  
  // DEBUG: Add detailed debugging if requested
  if (forceDebug) {
    await debugV2AttributionLookup(redis, conversionData);
  }
  
  const conversionTime = new Date(conversionData.timestamp);
  const allPageviews = [];
  const seenPageviews = new Set();
  const attributionMethods = [];
  const dataSourcesUsed = [];
  let targetIPDetected = false;
  
  // V2 Attribution Query 1: Session ID lookup (enhanced with verification data)
  if (conversionData.ssid) {
    console.log(`🔗 V2 Query 1: Enhanced session ID lookup for: ${conversionData.ssid}`);
    
    try {
      const sessionKey = `attribution_index_v2_session:${conversionData.ssid}`;
      const sessionResult = await redis(`get/${sessionKey}`, 3000);
      
      if (sessionResult?.result) {
        const sessionIndex = JSON.parse(decodeURIComponent(sessionResult.result));
        console.log(`✅ V2 Session index found: ${sessionIndex.pageview_count} pageviews`);
        console.log(`📊 Data sources: ${sessionIndex.data_sources?.join(', ') || 'unknown'}`);
        
        attributionMethods.push('v2_session_match');
        if (sessionIndex.data_sources) {
          dataSourcesUsed.push(...sessionIndex.data_sources);
        }
        
        const addedInfo = addV2PageviewsToJourney(
          sessionIndex.pageviews, 
          'v2_session_match', 
          allPageviews, 
          seenPageviews, 
          conversionTime
        );
        
        if (addedInfo.target_ip_found) targetIPDetected = true;
      } else {
        console.log('⚠️ No V2 session index found');
      }
    } catch (sessionError) {
      console.log('⚠️ V2 Session lookup error:', sessionError.message);
    }
  } else {
    console.log('⚠️ No session ID available for V2 lookup');
  }
  
  // V2 Attribution Query 2: Primary IP lookup (enhanced)
  if (conversionData.primary_ip) {
    console.log(`🌐 V2 Query 2: Enhanced primary IP lookup for: ${conversionData.primary_ip}`);
    
    try {
      const encodedPrimaryIP = encodeIPForKey(conversionData.primary_ip);
      const primaryIPKey = `attribution_index_v2_ip:${encodedPrimaryIP}`;
      const primaryIPResult = await redis(`get/${primaryIPKey}`, 3000);
      
      if (primaryIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(primaryIPResult.result));
        console.log(`✅ V2 Primary IP index found: ${ipIndex.pageview_count} pageviews`);
        console.log(`📊 Data sources: ${ipIndex.data_sources?.join(', ') || 'unknown'}`);
        
        if (!attributionMethods.includes('v2_primary_ip_match')) {
          attributionMethods.push('v2_primary_ip_match');
        }
        if (ipIndex.data_sources) {
          dataSourcesUsed.push(...ipIndex.data_sources);
        }
        
        const addedInfo = addV2PageviewsToJourney(
          ipIndex.pageviews, 
          'v2_primary_ip_match', 
          allPageviews, 
          seenPageviews, 
          conversionTime
        );
        
        if (addedInfo.target_ip_found) targetIPDetected = true;
      } else {
        console.log('⚠️ No V2 primary IP index found');
      }
    } catch (ipError) {
      console.log('⚠️ V2 Primary IP lookup error:', ipError.message);
    }
  }
  
  // V2 Attribution Query 3: Conversion IP lookup (enhanced cross-device)
  if (conversionData.conversion_ip && conversionData.conversion_ip !== conversionData.primary_ip) {
    console.log(`🌐 V2 Query 3: Enhanced conversion IP lookup for: ${conversionData.conversion_ip}`);
    
    try {
      const encodedConversionIP = encodeIPForKey(conversionData.conversion_ip);
      const conversionIPKey = `attribution_index_v2_ip:${encodedConversionIP}`;
      const conversionIPResult = await redis(`get/${conversionIPKey}`, 3000);
      
      if (conversionIPResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(conversionIPResult.result));
        console.log(`✅ V2 Conversion IP index found: ${ipIndex.pageview_count} pageviews`);
        console.log(`📊 Data sources: ${ipIndex.data_sources?.join(', ') || 'unknown'}`);
        
        if (!attributionMethods.includes('v2_conversion_ip_match')) {
          attributionMethods.push('v2_conversion_ip_match');
        }
        if (ipIndex.data_sources) {
          dataSourcesUsed.push(...ipIndex.data_sources);
        }
        
        const addedInfo = addV2PageviewsToJourney(
          ipIndex.pageviews, 
          'v2_conversion_ip_match', 
          allPageviews, 
          seenPageviews, 
          conversionTime
        );
        
        if (addedInfo.target_ip_found) targetIPDetected = true;
      } else {
        console.log('⚠️ No V2 conversion IP index found');
      }
    } catch (ipError) {
      console.log('⚠️ V2 Conversion IP lookup error:', ipError.message);
    }
  }
  
  // NEW V2 Attribution Query 4: Landing Page lookup
  if (conversionData.landing_page && conversionData.landing_page !== 'unknown') {
    console.log(`📄 V2 Query 4: Landing page attribution for: ${conversionData.landing_page}`);
    
    try {
      const encodedLP = encodeLandingPageForKey(conversionData.landing_page);
      const landingPageKey = `attribution_index_v2_landing:${encodedLP}`;
      const landingPageResult = await redis(`get/${landingPageKey}`, 3000);
      
      if (landingPageResult?.result) {
        const lpIndex = JSON.parse(decodeURIComponent(landingPageResult.result));
        console.log(`✅ V2 Landing page index found: ${lpIndex.pageview_count} pageviews`);
        console.log(`📊 Data sources: ${lpIndex.data_sources?.join(', ') || 'unknown'}`);
        
        if (!attributionMethods.includes('v2_landing_page_match')) {
          attributionMethods.push('v2_landing_page_match');
        }
        if (lpIndex.data_sources) {
          dataSourcesUsed.push(...lpIndex.data_sources);
        }
        
        const addedInfo = addV2PageviewsToJourney(
          lpIndex.pageviews, 
          'v2_landing_page_match', 
          allPageviews, 
          seenPageviews, 
          conversionTime
        );
        
        if (addedInfo.target_ip_found) targetIPDetected = true;
      } else {
        console.log('⚠️ No V2 landing page index found');
      }
    } catch (lpError) {
      console.log('⚠️ V2 Landing page lookup error:', lpError.message);
    }
  }
  
  // NEW V2 Attribution Query 5: Source lookup
  if (conversionData.source && conversionData.source !== 'direct' && conversionData.source !== 'unknown') {
    console.log(`📊 V2 Query 5: Source attribution for: ${conversionData.source}`);
    
    try {
      const encodedSource = encodeSourceForKey(conversionData.source);
      const sourceKey = `attribution_index_v2_source:${encodedSource}`;
      const sourceResult = await redis(`get/${sourceKey}`, 3000);
      
      if (sourceResult?.result) {
        const sourceIndex = JSON.parse(decodeURIComponent(sourceResult.result));
        console.log(`✅ V2 Source index found: ${sourceIndex.pageview_count} pageviews`);
        console.log(`📊 Data sources: ${sourceIndex.data_sources?.join(', ') || 'unknown'}`);
        
        if (!attributionMethods.includes('v2_source_match')) {
          attributionMethods.push('v2_source_match');
        }
        if (sourceIndex.data_sources) {
          dataSourcesUsed.push(...sourceIndex.data_sources);
        }
        
        const addedInfo = addV2PageviewsToJourney(
          sourceIndex.pageviews, 
          'v2_source_match', 
          allPageviews, 
          seenPageviews, 
          conversionTime
        );
        
        if (addedInfo.target_ip_found) targetIPDetected = true;
      } else {
        console.log('⚠️ No V2 source index found');
      }
    } catch (sourceError) {
      console.log('⚠️ V2 Source lookup error:', sourceError.message);
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
  
  // Calculate V2 enhanced attribution summary
  const attributionSummary = calculateV2AttributionSummary(
    customerJourney, 
    conversionData, 
    attributionMethods,
    [...new Set(dataSourcesUsed)],
    targetIPDetected
  );
  
  console.log(`🎯 V2 Attribution analysis complete:`);
  console.log(`   📊 Total touchpoints: ${customerJourney.length}`);
  console.log(`   🔍 Attribution methods: ${attributionMethods.join(', ')}`);
  console.log(`   📅 Journey duration: ${attributionSummary.journey_duration_days} days`);
  console.log(`   🎯 Target IP detected: ${targetIPDetected}`);
  console.log(`   📊 Data sources: ${attributionSummary.data_sources_used.join(', ')}`);
  
  return {
    conversion: {
      email: conversionData.email,
      timestamp: conversionData.timestamp,
      order_total: conversionData.order_total,
      landing_page: conversionData.landing_page,
      source: conversionData.source,
      conversion_ip: conversionData.conversion_ip,
      primary_ip: conversionData.primary_ip,
      ssid: conversionData.ssid
    },
    attribution_summary: attributionSummary,
    customer_journey: customerJourney
  };
}

// V2 Debug function to trace attribution lookup issues
async function debugV2AttributionLookup(redis, conversionData) {
  console.log('🔍 ================== V2 DEBUG ATTRIBUTION LOOKUP ==================');
  console.log('🔍 V2 DEBUG: Starting attribution lookup for:', {
    email: conversionData.email,
    timestamp: conversionData.timestamp,
    conversion_ip: conversionData.conversion_ip,
    primary_ip: conversionData.primary_ip,
    ssid: conversionData.ssid,
    landing_page: conversionData.landing_page,
    source: conversionData.source
  });

  // Debug 1: Check if V2 session index exists
  if (conversionData.ssid) {
    const sessionKey = `attribution_index_v2_session:${conversionData.ssid}`;
    console.log('🔍 V2 Checking session key:', sessionKey);
    
    try {
      const sessionResult = await redis(`get/${sessionKey}`, 3000);
      console.log('🔍 V2 Session result:', sessionResult ? 'FOUND' : 'NOT FOUND');
      if (sessionResult?.result) {
        const sessionIndex = JSON.parse(decodeURIComponent(sessionResult.result));
        console.log('🔍 V2 Session pageviews:', sessionIndex.pageview_count || 0);
        console.log('🔍 V2 Session data sources:', sessionIndex.data_sources || 'unknown');
        console.log('🔍 V2 Session latest timestamp:', sessionIndex.latest_timestamp);
        console.log('🔍 V2 Session earliest timestamp:', sessionIndex.earliest_timestamp);
      }
    } catch (error) {
      console.log('🔍 V2 Session lookup error:', error.message);
    }
  }

  // Debug 2: Check if V2 IP indexes exist
  const ipsToCheck = [conversionData.primary_ip, conversionData.conversion_ip].filter(Boolean);
  console.log('🔍 V2 IPs to check:', ipsToCheck);
  
  for (const ip of ipsToCheck) {
    const encodedIP = encodeIPForKey(ip);
    const ipKey = `attribution_index_v2_ip:${encodedIP}`;
    console.log('🔍 V2 Checking IP key:', ipKey);
    
    try {
      const ipResult = await redis(`get/${ipKey}`, 3000);
      console.log('🔍 V2 IP result for', ip, ':', ipResult ? 'FOUND' : 'NOT FOUND');
      if (ipResult?.result) {
        const ipIndex = JSON.parse(decodeURIComponent(ipResult.result));
        console.log('🔍 V2 IP pageviews:', ipIndex.pageview_count || 0);
        console.log('🔍 V2 IP data sources:', ipIndex.data_sources || 'unknown');
        console.log('🔍 V2 IP latest timestamp:', ipIndex.latest_timestamp);
        console.log('🔍 V2 IP earliest timestamp:', ipIndex.earliest_timestamp);
        
        // Check for target IP
        if (ip === '42.61.210.120') {
          console.log('🎯 V2 TARGET IP FOUND in index!');
        }
      }
    } catch (error) {
      console.log('🔍 V2 IP lookup error for', ip, ':', error.message);
    }
  }

  // Debug 3: Check V2 landing page index
  if (conversionData.landing_page && conversionData.landing_page !== 'unknown') {
    const encodedLP = encodeLandingPageForKey(conversionData.landing_page);
    const lpKey = `attribution_index_v2_landing:${encodedLP}`;
    console.log('🔍 V2 Checking landing page key:', lpKey);
    
    try {
      const lpResult = await redis(`get/${lpKey}`, 3000);
      console.log('🔍 V2 Landing page result:', lpResult ? 'FOUND' : 'NOT FOUND');
      if (lpResult?.result) {
        const lpIndex = JSON.parse(decodeURIComponent(lpResult.result));
        console.log('🔍 V2 Landing page pageviews:', lpIndex.pageview_count || 0);
        console.log('🔍 V2 Landing page data sources:', lpIndex.data_sources || 'unknown');
      }
    } catch (error) {
      console.log('🔍 V2 Landing page lookup error:', error.message);
    }
  }

  // Debug 4: Check V2 source index
  if (conversionData.source && conversionData.source !== 'direct' && conversionData.source !== 'unknown') {
    const encodedSource = encodeSourceForKey(conversionData.source);
    const sourceKey = `attribution_index_v2_source:${encodedSource}`;
    console.log('🔍 V2 Checking source key:', sourceKey);
    
    try {
      const sourceResult = await redis(`get/${sourceKey}`, 3000);
      console.log('🔍 V2 Source result:', sourceResult ? 'FOUND' : 'NOT FOUND');
      if (sourceResult?.result) {
        const sourceIndex = JSON.parse(decodeURIComponent(sourceResult.result));
        console.log('🔍 V2 Source pageviews:', sourceIndex.pageview_count || 0);
        console.log('🔍 V2 Source data sources:', sourceIndex.data_sources || 'unknown');
      }
    } catch (error) {
      console.log('🔍 V2 Source lookup error:', error.message);
    }
  }
  
  console.log('🔍 ================== END V2 DEBUG ATTRIBUTION LOOKUP ==================');
}

// Add V2 pageviews to journey with enhanced deduplication and target IP detection
function addV2PageviewsToJourney(pageviews, attributionMethod, allPageviews, seenPageviews, conversionTime) {
  let addedCount = 0;
  let filteredCount = 0;
  let targetIPFound = false;
  
  pageviews.forEach(pageview => {
    const pageviewTime = new Date(pageview.timestamp);
    
    // Only include pageviews that occurred BEFORE the conversion
    if (pageviewTime >= conversionTime) {
      filteredCount++;
      return;
    }
    
    // V2 Enhanced deduplication using session_id + timestamp + IP combination
    const dedupeKey = `${pageview.session_id}_${pageview.timestamp}_${pageview.ip_address}`;
    
    if (!seenPageviews.has(dedupeKey)) {
      seenPageviews.add(dedupeKey);
      
      // Check for target IP detection
      if (pageview.ip_address === '42.61.210.120') {
        targetIPFound = true;
        console.log(`🎯 TARGET IP DETECTED in ${attributionMethod}: ${pageview.timestamp}`);
      }
      
      // Check for verification recovery data
      const isVerificationData = pageview.source_pattern?.includes('verification') || 
                                pageview.redis_key?.includes('verification');
      
      allPageviews.push({
        ...pageview,
        attribution_method: attributionMethod,
        is_verification_data: isVerificationData
      });
      addedCount++;
    }
  });
  
  console.log(`   📝 V2 Added ${addedCount} pageviews via ${attributionMethod}, filtered ${filteredCount} future pageviews${targetIPFound ? ' 🎯' : ''}`);
  
  return {
    added_count: addedCount,
    filtered_count: filteredCount,
    target_ip_found: targetIPFound
  };
}

// Calculate V2 enhanced attribution summary
function calculateV2AttributionSummary(customerJourney, conversionData, attributionMethods, dataSourcesUsed, targetIPDetected) {
  if (customerJourney.length === 0) {
    return {
      total_touchpoints: 0,
      journey_duration_days: 0,
      attribution_methods_used: attributionMethods,
      data_sources_used: dataSourcesUsed,
      target_ip_detected: targetIPDetected,
      unique_sessions: 0,
      unique_sources: [],
      unique_campaigns: [],
      unique_landing_pages: [],
      verification_data_points: 0,
      first_touch: null,
      last_touch: null,
      attribution_confidence: calculateV2AttributionConfidence(customerJourney, conversionData, attributionMethods, dataSourcesUsed, targetIPDetected)
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
  const uniqueLandingPages = [...new Set(customerJourney.map(pv => pv.landing_page).filter(Boolean))];
  const verificationDataPoints = customerJourney.filter(pv => pv.is_verification_data).length;
  
  return {
    total_touchpoints: customerJourney.length,
    journey_duration_days: journeyDurationDays,
    attribution_methods_used: attributionMethods,
    data_sources_used: dataSourcesUsed,
    target_ip_detected: targetIPDetected,
    unique_sessions: uniqueSessions.length,
    unique_sources: uniqueSources,
    unique_campaigns: uniqueCampaigns,
    unique_landing_pages: uniqueLandingPages,
    verification_data_points: verificationDataPoints,
    landing_page_progression: analyzeLandingPageProgression(customerJourney, conversionData),
    source_progression: analyzeSourceProgression(customerJourney, conversionData),
    first_touch: {
      timestamp: firstTouch.timestamp,
      landing_page: firstTouch.landing_page,
      source: firstTouch.source,
      utm_campaign: firstTouch.utm_campaign,
      attribution_method: firstTouch.attribution_method,
      is_verification_data: firstTouch.is_verification_data
    },
    last_touch: {
      timestamp: lastTouch.timestamp,
      landing_page: lastTouch.landing_page,
      source: lastTouch.source,
      utm_campaign: lastTouch.utm_campaign,
      attribution_method: lastTouch.attribution_method,
      is_verification_data: lastTouch.is_verification_data
    },
    attribution_confidence: calculateV2AttributionConfidence(customerJourney, conversionData, attributionMethods, dataSourcesUsed, targetIPDetected)
  };
}

// NEW: Analyze landing page progression through customer journey
function analyzeLandingPageProgression(customerJourney, conversionData) {
  const landingPageFlow = customerJourney.map(pv => pv.landing_page).filter(Boolean);
  const uniqueFlow = [...new Set(landingPageFlow)];
  
  return {
    landing_page_sequence: landingPageFlow,
    unique_landing_pages: uniqueFlow,
    conversion_landing_page: conversionData.landing_page,
    landing_page_consistency: conversionData.landing_page && uniqueFlow.includes(conversionData.landing_page)
  };
}

// NEW: Analyze source progression through customer journey
function analyzeSourceProgression(customerJourney, conversionData) {
  const sourceFlow = customerJourney.map(pv => pv.source).filter(Boolean);
  const uniqueFlow = [...new Set(sourceFlow)];
  
  return {
    source_sequence: sourceFlow,
    unique_sources: uniqueFlow,
    conversion_source: conversionData.source,
    source_consistency: conversionData.source && uniqueFlow.includes(conversionData.source),
    cross_source_journey: uniqueFlow.length > 1
  };
}

// Calculate V2 enhanced attribution confidence score
function calculateV2AttributionConfidence(customerJourney, conversionData, attributionMethods, dataSourcesUsed, targetIPDetected) {
  let score = 0;
  const factors = {};
  
  // Session ID available (high confidence)
  if (conversionData.ssid && attributionMethods.includes('v2_session_match')) {
    score += 35;
    factors.session_id_available = true;
  } else {
    factors.session_id_available = false;
  }
  
  // Cross-device detection
  const crossDeviceDetected = conversionData.conversion_ip !== conversionData.primary_ip;
  factors.cross_device_detected = crossDeviceDetected;
  if (crossDeviceDetected) {
    score += 20;
  }
  
  // V2 NEW: Landing page attribution available
  if (attributionMethods.includes('v2_landing_page_match')) {
    score += 10;
    factors.landing_page_attribution = true;
  } else {
    factors.landing_page_attribution = false;
  }
  
  // V2 NEW: Source attribution available
  if (attributionMethods.includes('v2_source_match')) {
    score += 10;
    factors.source_attribution = true;
  } else {
    factors.source_attribution = false;
  }
  
  // V2 NEW: Verification data quality
  const verificationDataPoints = customerJourney.filter(pv => pv.is_verification_data).length;
  if (verificationDataPoints > 0) {
    score += 10;
    factors.verification_data_recovered = true;
    factors.verification_data_points = verificationDataPoints;
  } else {
    factors.verification_data_recovered = false;
  }
  
  // V2 NEW: Target IP detection
  if (targetIPDetected) {
    score += 5;
    factors.target_ip_detected = true;
  } else {
    factors.target_ip_detected = false;
  }
  
  // V2 NEW: Enhanced data sources
  if (dataSourcesUsed.includes('enhanced_extraction_v2')) {
    score += 5;
    factors.enhanced_extraction_data = true;
  }
  if (dataSourcesUsed.includes('verification_recovery_v2')) {
    score += 5;
    factors.verification_recovery_data = true;
  }
  
  // Journey completeness (enhanced scoring)
  if (customerJourney.length >= 10) {
    score += 25;
    factors.journey_completeness = 'excellent';
  } else if (customerJourney.length >= 5) {
    score += 15;
    factors.journey_completeness = 'high';
  } else if (customerJourney.length >= 2) {
    score += 8;
    factors.journey_completeness = 'medium';
  } else {
    factors.journey_completeness = 'low';
  }
  
  // V2 Multiple attribution methods (enhanced for 5 methods)
  if (attributionMethods.length >= 4) {
    score += 20;
    factors.attribution_method_coverage = 'excellent';
  } else if (attributionMethods.length >= 3) {
    score += 15;
    factors.attribution_method_coverage = 'high';
  } else if (attributionMethods.length >= 2) {
    score += 10;
    factors.attribution_method_coverage = 'medium';
  } else {
    factors.attribution_method_coverage = 'low';
  }
  
  // Temporal accuracy
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
    factors: factors,
    version: 'v2_enhanced'
  };
}

// Store V2 attribution result permanently in Redis
async function storeV2AttributionResult(redis, attributionResult) {
  const storageKey = `multi_touch_attribution_v2:${attributionResult.conversion.email}:${attributionResult.conversion.timestamp}`;
  
  // Add V2 storage metadata
  const enrichedResult = {
    ...attributionResult,
    storage_metadata: {
      stored_at: new Date().toISOString(),
      storage_key: storageKey,
      attribution_version: 'v2_enhanced',
      storage_permanent: true,
      data_sources_processed: attributionResult.attribution_summary.data_sources_used,
      attribution_methods_count: attributionResult.attribution_summary.attribution_methods_used.length
    }
  };
  
  try {
    console.log('💾 Storing permanent V2 attribution result:', storageKey);
    
    // PERMANENT storage - NO TTL specified
    const setResult = await redis(`set/${storageKey}/${encodeURIComponent(JSON.stringify(enrichedResult))}`);
    console.log('📋 V2 Redis set returned:', JSON.stringify(setResult));
    
    // Verify storage
    console.log('🔍 Verifying V2 attribution write with get command...');
    const verifyResult = await redis(`get/${storageKey}`, 3000);
    console.log('📋 V2 Redis get verification:', verifyResult ? 'DATA FOUND ✅' : 'DATA NOT FOUND ❌');
    
    if (verifyResult && verifyResult.result) {
      console.log('✅ VERIFIED: V2 Attribution result successfully written and readable (PERMANENT STORAGE)');
      return { 
        success: true, 
        key: storageKey, 
        verified: true 
      };
    } else {
      console.log('❌ CRITICAL: V2 Attribution result not found after set command - Redis write failed silently');
      return { 
        success: false, 
        error: 'V2 Attribution result not found after write attempt - silent Redis failure', 
        verified: false 
      };
    }
    
  } catch (error) {
    console.log('❌ V2 Attribution storage operation failed:', error.message);
    return { 
      success: false, 
      error: error.message, 
      verified: false 
    };
  }
}

// Utility functions for encoding keys (consistent with V2 index builders)
function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_').replace(/\./g, '_');
}

function encodeLandingPageForKey(landingPage) {
  return encodeURIComponent(landingPage).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 100);
}

function encodeSourceForKey(source) {
  return encodeURIComponent(source).replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 50);
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
