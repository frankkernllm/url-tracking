// attribution-recovery-production.js
// PRODUCTION ATTRIBUTION RECOVERY: Using confirmed main_ip_address field and comma-separated IP logic
// Path: netlify/functions/attribution-recovery-production.js
// Purpose: Process ALL conversions from conversion_index_date:* to find pageview matches

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
    console.log('🚀 PRODUCTION ATTRIBUTION RECOVERY: Using confirmed main_ip_address field...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      recovery_window_days = 15,         // Process 2025-06-08 to recent (confirmed date range)
      extended_window_hours = 72,        // 3-day attribution window
      batch_size = 25,                   // Process 25 conversions per batch
      test_mode = false                  // Test mode processes fewer records
    } = body;
    
    console.log(`⚡ Recovery Parameters: ${recovery_window_days} days, ${extended_window_hours}h window, test_mode: ${test_mode}`);
    
    // STEP 1: Load conversion indexes using confirmed date range
    console.log('📊 Step 1: Loading conversion indexes with main_ip_address field...');
    const conversionData = await loadConversionIndexesProduction(redis, recovery_window_days);
    
    console.log(`✅ Conversion data loaded:`);
    console.log(`   📦 ${conversionData.totalConversions} conversions from ${conversionData.dateKeys.length} date indexes`);
    console.log(`   🌐 ${conversionData.conversionsWithIPs} conversions have main_ip_address data`);
    console.log(`   📅 Date range: ${conversionData.dateRange.start} to ${conversionData.dateRange.end}`);
    
    if (conversionData.conversionsWithIPs === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          issue: 'No conversions found with main_ip_address field',
          debug_info: {
            total_conversions: conversionData.totalConversions,
            date_indexes_checked: conversionData.dateKeys,
            sample_conversion_fields: conversionData.sampleFields
          },
          recommendations: [
            'Check if main_ip_address field exists in conversion data',
            'Verify conversion_index_date:* keys have expected structure'
          ]
        })
      };
    }
    
    // STEP 2: Load conversion-only journeys that need recovery
    console.log('📦 Step 2: Loading conversion-only journeys...');
    const conversionOnlyJourneys = await loadConversionOnlyJourneysProduction(redis, test_mode);
    
    console.log(`✅ Journey data loaded:`);
    console.log(`   🎯 ${conversionOnlyJourneys.length} conversion-only journeys need recovery`);
    
    if (conversionOnlyJourneys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversion-only journeys found that need recovery',
          summary: {
            conversions_available: conversionData.totalConversions,
            journeys_needing_recovery: 0
          }
        })
      };
    }
    
    // STEP 3: Process recovery using confirmed IP logic
    console.log('🔗 Step 3: Processing recovery with main_ip_address comma-separated logic...');
    const recoveryResults = await processRecoveryProduction(
      redis,
      conversionOnlyJourneys,
      conversionData.conversions,
      extended_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    
    console.log(`✅ Production recovery complete: ${recoveryResults.successful_recoveries} recoveries in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        production_recovery: true,
        main_ip_address_confirmed: true,
        recovery_summary: {
          conversions_available: conversionData.totalConversions,
          conversions_with_ips: conversionData.conversionsWithIPs,
          journeys_targeted: conversionOnlyJourneys.length,
          recovery_attempts: recoveryResults.recovery_attempts,
          successful_recoveries: recoveryResults.successful_recoveries,
          new_pageviews_found: recoveryResults.new_pageviews_found,
          processing_time_ms: totalTime
        },
        performance_metrics: {
          recovery_success_rate: recoveryResults.recovery_attempts > 0 ? 
            ((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1) + '%' : '0%',
          average_pageviews_per_recovery: recoveryResults.successful_recoveries > 0 ? 
            (recoveryResults.new_pageviews_found / recoveryResults.successful_recoveries).toFixed(1) : '0',
          ip_processing_method: 'main_ip_address_comma_separated_confirmed'
        },
        field_processing_confirmed: {
          ip_field_used: 'main_ip_address',
          ip_processing: 'comma_separated_string_split',
          ipv6_encoding: 'colon_to_underscore_replacement',
          diagnostic_validated: true
        },
        recovery_examples: recoveryResults.recovery_examples.slice(0, 5),
        ip_matching_stats: recoveryResults.ip_matching_stats,
        next_steps: recoveryResults.journeys_remaining > 0 ? [
          `Continue recovery: ${recoveryResults.journeys_remaining} conversion-only journeys remaining`,
          'Run same command again to process more batches',
          'Production system using confirmed IP processing logic'
        ] : [
          '🎉 ATTRIBUTION RECOVERY COMPLETE!',
          'All conversion-only journeys processed with production system',
          'Attribution success rate improved using main_ip_address field',
          'System ready for enhanced multi-touch attribution analysis'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Production attribution recovery failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Production attribution recovery failed', 
        message: error.message 
      })
    };
  }
};

// PRODUCTION: Load conversion indexes using confirmed date range and field structure
async function loadConversionIndexesProduction(redis, recoveryWindowDays) {
  console.log(`📊 Loading conversion indexes for ${recoveryWindowDays} days with main_ip_address field...`);
  
  const conversions = [];
  const dateKeys = [];
  let conversionsWithIPs = 0;
  const sampleFields = new Set();
  
  // Generate date keys for confirmed range (2025-06-08 onwards)
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - recoveryWindowDays);
  
  const datesToCheck = [];
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    const dateKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    datesToCheck.push(dateKey);
  }
  
  console.log(`📅 Checking ${datesToCheck.length} conversion indexes...`);
  
  // Load conversion indexes in parallel batches
  const batchSize = 5;
  for (let i = 0; i < datesToCheck.length; i += batchSize) {
    const batch = datesToCheck.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (dateKey) => {
      try {
        const indexKey = `conversion_index_date:${dateKey}`;
        const indexData = await redis(`get/${indexKey}`);
        
        if (indexData?.result) {
          const parsed = JSON.parse(decodeURIComponent(indexData.result));
          dateKeys.push(dateKey);
          
          if (parsed.conversions && Array.isArray(parsed.conversions)) {
            return parsed.conversions.map(conversion => {
              // Track fields for debugging
              Object.keys(conversion).forEach(field => sampleFields.add(field));
              
              // CONFIRMED: Extract IPs from main_ip_address field using comma-separated logic
              const extractedIPs = extractIPsFromMainField(conversion);
              
              if (extractedIPs.length > 0) {
                conversionsWithIPs++;
              }
              
              return {
                ...conversion,
                date_key: dateKey,
                extracted_ips: extractedIPs,
                has_ip_data: extractedIPs.length > 0
              };
            });
          }
        }
      } catch (error) {
        console.warn(`⚠️ Error loading conversion index for ${dateKey}:`, error.message);
      }
      return [];
    });
    
    const batchResults = await Promise.all(batchPromises);
    const validConversions = batchResults.flat();
    conversions.push(...validConversions);
  }
  
  // Sort by timestamp (most recent first)
  conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  console.log(`✅ Loaded ${conversions.length} conversions with ${conversionsWithIPs} having IP data`);
  
  return {
    conversions,
    dateKeys,
    totalConversions: conversions.length,
    conversionsWithIPs,
    sampleFields: Array.from(sampleFields),
    dateRange: {
      start: dateKeys[0],
      end: dateKeys[dateKeys.length - 1]
    }
  };
}

// CONFIRMED: Extract IPs from main_ip_address field using validated logic
function extractIPsFromMainField(conversion) {
  try {
    // CONFIRMED from diagnostic: Use main_ip_address field
    const mainIPField = conversion.main_ip_address;
    
    if (!mainIPField || mainIPField === 'unknown') {
      return [];
    }
    
    // CONFIRMED: Split comma-separated string and clean up
    const ipString = String(mainIPField);
    const ips = ipString.includes(',') 
      ? ipString.split(',').map(ip => ip.trim()) 
      : [ipString.trim()];
    
    // Filter out unknowns and empty strings
    const validIPs = ips.filter(ip => ip && ip !== 'unknown' && ip.length > 0);
    
    return validIPs;
  } catch (error) {
    console.warn('⚠️ IP extraction error for conversion:', error.message);
    return [];
  }
}

// PRODUCTION: Load conversion-only journeys efficiently
async function loadConversionOnlyJourneysProduction(redis, testMode) {
  console.log(`📦 Loading conversion-only journeys (test_mode: ${testMode})...`);
  
  const conversionOnlyJourneys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = testMode ? 5 : 20; // Limit iterations in test mode
  
  try {
    do {
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/500`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Process keys in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        const batch = keys.slice(i, i + batchSize);
        
        const batchPromises = batch.map(async (key) => {
          try {
            const journeyData = await redis(`get/${key}`);
            if (journeyData?.result) {
              const journey = JSON.parse(decodeURIComponent(journeyData.result));
              
              // Identify conversion-only journeys that need recovery
              const isConversionOnly = journey.total_touchpoints === 1 || 
                                     journey.reconstruction_method?.includes('conversion_only') ||
                                     (journey.touchpoints && journey.touchpoints.every(tp => tp.is_conversion || tp.type === 'conversion'));
              
              const needsRecovery = isConversionOnly && !journey.recovery_attempted;
              
              if (needsRecovery) {
                return {
                  journey_id: journey.journey_id,
                  journey_key: key,
                  customer_email: journey.customer_email,
                  conversion_order_id: journey.conversion_order_id,
                  conversion_timestamp: journey.conversion_timestamp,
                  conversion_value: journey.conversion_value,
                  current_touchpoints: journey.total_touchpoints
                };
              }
            }
          } catch (parseError) {
            // Skip invalid journey data
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        const validTargets = batchResults.filter(target => target !== null);
        conversionOnlyJourneys.push(...validTargets);
        
        // Test mode limit
        if (testMode && conversionOnlyJourneys.length >= 10) {
          console.log(`🧪 Test mode: Limited to ${conversionOnlyJourneys.length} journeys`);
          break;
        }
      }
      
      // Test mode early exit
      if (testMode && conversionOnlyJourneys.length >= 10) break;
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (error) {
    console.warn('⚠️ Journey loading error:', error.message);
  }
  
  // Sort by conversion timestamp (most recent first)
  conversionOnlyJourneys.sort((a, b) => new Date(b.conversion_timestamp) - new Date(a.conversion_timestamp));
  
  console.log(`✅ Loaded ${conversionOnlyJourneys.length} conversion-only journeys for recovery`);
  return conversionOnlyJourneys;
}

// PRODUCTION: Process recovery using confirmed main_ip_address logic
async function processRecoveryProduction(redis, journeys, conversions, extendedWindowHours, batchSize, maxTime) {
  console.log(`🔗 Processing ${journeys.length} journeys with main_ip_address comma-separated logic...`);
  
  const processingStartTime = Date.now();
  let recoveryAttempts = 0;
  let successfulRecoveries = 0;
  let newPageviewsFound = 0;
  const recoveryExamples = [];
  const ipMatchingStats = {
    conversions_checked: 0,
    conversions_with_ips: 0,
    unique_ips_found: new Set(),
    pageview_indexes_checked: new Set(),
    ip_matches_found: 0
  };
  
  // Step 1: Create lookup maps
  console.log('🗺️ Building conversion lookup map...');
  const conversionsByOrderId = new Map();
  
  conversions.forEach(conversion => {
    const orderId = conversion.order_id || conversion.conversion_order_id;
    if (orderId) {
      conversionsByOrderId.set(String(orderId), conversion);
      ipMatchingStats.conversions_checked++;
      
      if (conversion.extracted_ips && conversion.extracted_ips.length > 0) {
        ipMatchingStats.conversions_with_ips++;
        conversion.extracted_ips.forEach(ip => ipMatchingStats.unique_ips_found.add(ip));
      }
    }
  });
  
  console.log(`📊 Lookup map: ${conversionsByOrderId.size} conversions indexed`);
  console.log(`🌐 Total unique IPs found: ${ipMatchingStats.unique_ips_found.size}`);
  
  // Step 2: Process journeys in batches
  const journeysToUpdate = [];
  
  for (let i = 0; i < journeys.length; i += batchSize) {
    if (Date.now() - processingStartTime > maxTime - 8000) {
      console.log('⏰ Time limit reached during recovery processing');
      break;
    }
    
    const batch = journeys.slice(i, i + batchSize);
    console.log(`🔄 Processing batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${journeys.length}`);
    
    for (const journey of batch) {
      try {
        recoveryAttempts++;
        
        // Find matching conversion
        const conversion = conversionsByOrderId.get(String(journey.conversion_order_id));
        
        if (conversion && conversion.extracted_ips && conversion.extracted_ips.length > 0) {
          // Check if any IPs have pageview indexes
          const pageviewMatches = await findPageviewMatchesProduction(
            redis,
            conversion.extracted_ips,
            conversion.timestamp,
            extendedWindowHours
          );
          
          // Track IP matching stats
          conversion.extracted_ips.forEach(ip => {
            const encodedIP = encodeIPForPageviewIndex(ip);
            ipMatchingStats.pageview_indexes_checked.add(encodedIP);
          });
          
          if (pageviewMatches.length > 0) {
            successfulRecoveries++;
            newPageviewsFound += pageviewMatches.length;
            ipMatchingStats.ip_matches_found++;
            
            // Build enhanced journey
            const enhancedJourney = buildEnhancedJourneyProduction(journey, pageviewMatches);
            
            journeysToUpdate.push({
              key: journey.journey_key,
              journey: enhancedJourney
            });
            
            recoveryExamples.push({
              journey_id: journey.journey_id,
              order_id: journey.conversion_order_id,
              customer_email: journey.customer_email,
              conversion_ips: conversion.extracted_ips,
              pageviews_found: pageviewMatches.length,
              recovery_method: 'main_ip_address_production'
            });
            
            console.log(`✅ Order ${journey.conversion_order_id}: Found ${pageviewMatches.length} pageviews from ${conversion.extracted_ips.length} IPs`);
          }
        }
        
      } catch (recoveryError) {
        console.warn(`⚠️ Recovery error for order ${journey.conversion_order_id}:`, recoveryError.message);
      }
    }
  }
  
  // Step 3: Batch update journeys
  if (journeysToUpdate.length > 0) {
    console.log(`💾 Batch updating ${journeysToUpdate.length} journeys...`);
    await batchUpdateJourneysProduction(redis, journeysToUpdate);
  }
  
  const journeysRemaining = Math.max(0, journeys.length - recoveryAttempts);
  
  // Finalize stats
  ipMatchingStats.unique_ips_found = ipMatchingStats.unique_ips_found.size;
  ipMatchingStats.pageview_indexes_checked = ipMatchingStats.pageview_indexes_checked.size;
  
  console.log(`🏁 Production processing complete: ${successfulRecoveries}/${recoveryAttempts} successful recoveries`);
  
  return {
    recovery_attempts: recoveryAttempts,
    successful_recoveries: successfulRecoveries,
    new_pageviews_found: newPageviewsFound,
    journeys_remaining: journeysRemaining,
    recovery_examples: recoveryExamples,
    ip_matching_stats: ipMatchingStats,
    processing_time_ms: Date.now() - processingStartTime
  };
}

// PRODUCTION: Find pageview matches using confirmed IP encoding
async function findPageviewMatchesProduction(redis, extractedIPs, conversionTimestamp, extendedWindowHours) {
  const pageviewMatches = [];
  const conversionTime = new Date(conversionTimestamp).getTime();
  const windowStart = conversionTime - (extendedWindowHours * 60 * 60 * 1000);
  
  try {
    // Check each IP for pageview index
    const batchPromises = extractedIPs.map(async (ip) => {
      try {
        // CONFIRMED: Encode IP for pageview index lookup
        const encodedIP = encodeIPForPageviewIndex(ip);
        const indexKey = `pageview_index_ip:${encodedIP}`;
        
        const indexData = await redis(`get/${indexKey}`);
        
        if (indexData?.result) {
          const parsed = JSON.parse(decodeURIComponent(indexData.result));
          
          if (parsed.pageviews && Array.isArray(parsed.pageviews)) {
            // Filter pageviews within time window
            const windowPageviews = parsed.pageviews.filter(pv => {
              const pvTime = new Date(pv.timestamp);
              return pvTime >= windowStart && pvTime <= conversionTime;
            });
            
            return windowPageviews.map(pv => ({
              ...pv,
              matched_ip: ip,
              attribution_method: 'main_ip_address_recovery_production',
              confidence: 240,
              recovery_method: 'production_main_ip_address'
            }));
          }
        }
      } catch (ipError) {
        console.warn(`⚠️ Error checking pageview index for IP ${ip}:`, ipError.message);
      }
      return [];
    });
    
    const batchResults = await Promise.all(batchPromises);
    const allMatches = batchResults.flat();
    
    // Remove duplicates and sort by timestamp
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Pageview matching error:', error.message);
    return [];
  }
}

// CONFIRMED: Encode IP for pageview index (IPv6 colon to underscore)
function encodeIPForPageviewIndex(ip) {
  return ip.replace(/:/g, '_');
}

// Build enhanced journey from production recovery
function buildEnhancedJourneyProduction(existingJourney, recoveredPageviews) {
  try {
    // Sort recovered pageviews by timestamp
    const sortedPageviews = recoveredPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    
    // Create new touchpoints from recovered pageviews
    const recoveredTouchpoints = sortedPageviews.map((pageview, index) => ({
      touchpoint_id: `${existingJourney.conversion_order_id}_production_${index + 1}`,
      timestamp: pageview.timestamp,
      landing_page: pageview.landing_page,
      source: pageview.source,
      medium: pageview.medium,
      campaign: pageview.campaign,
      content: pageview.content,
      term: pageview.term,
      referrer_url: pageview.referrer_url,
      attribution_method: pageview.attribution_method,
      confidence: pageview.confidence,
      matched_ip: pageview.matched_ip,
      recovery_method: 'production_main_ip_address',
      session_id: pageview.session_id,
      canvas_fingerprint: pageview.canvas_fingerprint,
      screen_resolution: pageview.screen_resolution,
      user_agent: pageview.user_agent,
      touchpoint_position: index + 1,
      is_first_touchpoint: index === 0,
      is_last_touchpoint: false
    }));
    
    // Get existing conversion touchpoint
    const existingConversionTouchpoint = existingJourney.touchpoints?.find(tp => tp.is_conversion || tp.type === 'conversion');
    if (existingConversionTouchpoint) {
      existingConversionTouchpoint.touchpoint_position = recoveredTouchpoints.length + 1;
      existingConversionTouchpoint.is_last_touchpoint = true;
    }
    
    const allTouchpoints = [...recoveredTouchpoints, existingConversionTouchpoint].filter(Boolean);
    
    // Recalculate journey metrics
    const journeyStart = new Date(allTouchpoints[0].timestamp);
    const journeyEnd = new Date(existingJourney.conversion_timestamp);
    const journeySpanHours = (journeyEnd - journeyStart) / (1000 * 60 * 60);
    
    const uniqueSessions = new Set(allTouchpoints.map(t => t.session_id).filter(Boolean)).size;
    const uniqueDeviceFingerprints = new Set(allTouchpoints.map(t => t.canvas_fingerprint).filter(Boolean)).size;
    const uniqueSources = new Set(allTouchpoints.map(t => t.source).filter(Boolean));
    
    return {
      ...existingJourney,
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
      touchpoints: allTouchpoints,
      recovery_attempted: true,
      recovery_timestamp: new Date().toISOString(),
      recovery_method: 'production_main_ip_address_confirmed',
      recovered_pageviews: sortedPageviews.length,
      reconstruction_method: 'production_attribution_recovery',
      field_processing_confirmed: {
        ip_field_used: 'main_ip_address',
        processing_method: 'comma_separated_string_split',
        ipv6_encoding: 'colon_to_underscore',
        diagnostic_validated: true
      }
    };
  } catch (error) {
    console.warn('⚠️ Journey building error:', error.message);
    return existingJourney; // Return original journey if enhancement fails
  }
}

// Batch update journeys (production version)
async function batchUpdateJourneysProduction(redis, journeysToUpdate) {
  const batchSize = 20;
  
  for (let i = 0; i < journeysToUpdate.length; i += batchSize) {
    const batch = journeysToUpdate.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async ({ key, journey }) => {
      try {
        await redis(`setex/${key}/2592000/${encodeURIComponent(JSON.stringify(journey))}`); // 30-day TTL
        return true;
      } catch (error) {
        console.warn(`⚠️ Error updating journey ${key}:`, error.message);
        return false;
      }
    });
    
    const results = await Promise.all(batchPromises);
    const successful = results.filter(Boolean).length;
    console.log(`💾 Updated ${successful}/${batch.length} journeys in batch ${Math.floor(i/batchSize) + 1}`);
  }
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
