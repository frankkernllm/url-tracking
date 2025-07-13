// Attribution Recovery Engine - Re-process conversion-only journeys with enhanced attribution
// Path: netlify/functions/attribution-recovery-engine.js
// Purpose: Find missed pageview attributions using current dual IP extraction logic

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
    console.log('🔄 ATTRIBUTION RECOVERY ENGINE: Starting recovery of missed attributions...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Get parameters
    const body = event.body ? JSON.parse(event.body) : {};
    const {
      extended_window_hours = 72,    // 72-hour extended window vs 7-day standard
      batch_size = 10,               // Smaller batches for careful processing
      force_reprocess = false        // Reprocess even if already attempted
    } = body;
    
    console.log(`🎯 Recovery Parameters: ${extended_window_hours}h window, batch size: ${batch_size}`);
    
    // Step 1: Find conversion-only journeys that need recovery
    const recoveryTargets = await findConversionOnlyJourneys(redis, force_reprocess, maxProcessingTime - (Date.now() - startTime));
    console.log(`🎯 Found ${recoveryTargets.length} conversion-only journeys for recovery processing`);
    
    if (recoveryTargets.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          recovery_complete: true,
          message: 'No conversion-only journeys found that need recovery processing',
          summary: {
            conversion_only_journeys_found: 0,
            recovery_already_complete: true
          }
        })
      };
    }
    
    // Step 2: Load original conversion data for enhanced IP extraction
    const conversionsWithOriginalData = await loadOriginalConversionData(redis, recoveryTargets, maxProcessingTime - (Date.now() - startTime));
    console.log(`💾 Loaded original conversion data for ${conversionsWithOriginalData.length} journeys`);
    
    // Step 3: Apply enhanced attribution recovery
    const recoveryResults = await processEnhancedAttributionRecovery(
      redis, 
      conversionsWithOriginalData, 
      extended_window_hours,
      batch_size,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Attribution recovery complete: ${recoveryResults.successful_recoveries} recoveries in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_summary: {
          conversion_only_journeys_targeted: recoveryTargets.length,
          conversions_with_original_data: conversionsWithOriginalData.length,
          recovery_attempts: recoveryResults.recovery_attempts,
          successful_recoveries: recoveryResults.successful_recoveries,
          additional_pageviews_found: recoveryResults.additional_pageviews_found,
          processing_time_ms: totalTime
        },
        recovery_performance: {
          recovery_success_rate: recoveryResults.recovery_attempts > 0 ? 
            ((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1) + '%' : '0%',
          average_pageviews_per_recovery: recoveryResults.successful_recoveries > 0 ? 
            (recoveryResults.additional_pageviews_found / recoveryResults.successful_recoveries).toFixed(1) : '0',
          processing_efficiency: 'enhanced_dual_ip_extraction'
        },
        attribution_improvements: {
          new_multi_touchpoint_journeys: recoveryResults.successful_recoveries,
          estimated_attribution_rate_improvement: recoveryResults.recovery_attempts > 0 ? 
            `+${((recoveryResults.successful_recoveries / recoveryResults.recovery_attempts) * 100).toFixed(1)}%` : '+0%'
        },
        recovery_details: recoveryResults.recovery_details.slice(0, 10), // First 10 examples
        next_steps: recoveryResults.journeys_remaining > 0 ? [
          `Continue recovery: ${recoveryResults.journeys_remaining} conversion-only journeys remaining`,
          'Run same command again to continue processing',
          'Each run will find additional attributions using enhanced IP logic'
        ] : [
          '🎉 Attribution recovery processing complete!',
          'All conversion-only journeys have been processed with enhanced attribution logic',
          'Use query-customer-journeys.js to see improved attribution rates',
          'System ready for multi-touch attribution analysis with recovered data'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Attribution recovery failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Attribution recovery failed', 
        message: error.message 
      })
    };
  }
};

// Find conversion-only customer journeys that need recovery
async function findConversionOnlyJourneys(redis, forceReprocess, maxTime) {
  console.log('🔍 Scanning for conversion-only journeys needing recovery...');
  
  const conversionOnlyJourneys = [];
  const scanStartTime = Date.now();
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
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
      iterations++;
      
      // Load and filter journey data in batches
      const batchSize = 50;
      for (let i = 0; i < keys.length; i += batchSize) {
        if (Date.now() - scanStartTime > maxTime - 2000) break;
        
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
                  current_touchpoints: journey.total_touchpoints,
                  recovery_attempted: journey.recovery_attempted || false
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
        
        if (conversionOnlyJourneys.length % 100 === 0 && conversionOnlyJourneys.length > 0) {
          console.log(`🎯 Recovery scan progress: ${conversionOnlyJourneys.length} conversion-only journeys found`);
        }
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
  } catch (scanError) {
    console.log(`⚠️ Journey scan error: ${scanError.message}`);
  }
  
  // Sort by conversion timestamp (most recent first)
  conversionOnlyJourneys.sort((a, b) => new Date(b.conversion_timestamp) - new Date(a.conversion_timestamp));
  
  console.log(`✅ Found ${conversionOnlyJourneys.length} conversion-only journeys for recovery`);
  return conversionOnlyJourneys;
}

// Load original conversion data for enhanced IP extraction
async function loadOriginalConversionData(redis, recoveryTargets, maxTime) {
  console.log(`📥 Loading original conversion data for ${recoveryTargets.length} journeys...`);
  
  const loadStartTime = Date.now();
  const conversionsWithData = [];
  
  // Process in batches to avoid timeout
  const batchSize = 25;
  for (let i = 0; i < recoveryTargets.length; i += batchSize) {
    if (Date.now() - loadStartTime > maxTime - 2000) {
      console.log('⏰ Time limit during conversion data loading, stopping');
      break;
    }
    
    const batch = recoveryTargets.slice(i, i + batchSize);
    
    const batchPromises = batch.map(async (target) => {
      try {
        // Find original conversion data by order_id
        const conversionKey = await findConversionByOrderId(redis, target.conversion_order_id);
        
        if (conversionKey) {
          const conversionData = await redis(`get/${conversionKey}`);
          if (conversionData?.result) {
            const originalConversion = JSON.parse(decodeURIComponent(conversionData.result));
            
            return {
              ...target,
              original_conversion_key: conversionKey,
              original_conversion_data: originalConversion,
              
              // Extract any additional IP data that may have been missed
              enhanced_ip_extraction: extractEnhancedIPData(originalConversion),
              geographic_data: {
                city: originalConversion.city,
                region: originalConversion.region,
                isp: originalConversion.isp,
                country: originalConversion.country
              }
            };
          }
        }
        
        return null;
      } catch (error) {
        console.warn(`⚠️ Error loading conversion data for order ${target.conversion_order_id}:`, error.message);
        return null;
      }
    });
    
    const batchResults = await Promise.all(batchPromises);
    const validResults = batchResults.filter(result => result !== null);
    conversionsWithData.push(...validResults);
    
    console.log(`📥 Data loading progress: ${conversionsWithData.length}/${recoveryTargets.length} conversions loaded`);
  }
  
  console.log(`✅ Loaded original data for ${conversionsWithData.length} conversions`);
  return conversionsWithData;
}

// Find conversion by order_id (scan conversions:* keys)
async function findConversionByOrderId(redis, orderId) {
  try {
    // Quick scan for conversion with matching order_id
    let cursor = '0';
    let iterations = 0;
    const maxIterations = 5;
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/200`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Check keys in batch
      for (const key of keys) {
        try {
          const conversionData = await redis(`get/${key}`);
          if (conversionData?.result) {
            const conversion = JSON.parse(decodeURIComponent(conversionData.result));
            if (conversion.order_id == orderId) {
              return key;
            }
          }
        } catch (e) {
          // Skip invalid data
        }
      }
      
    } while (cursor !== '0' && iterations < maxIterations);
    
    return null;
  } catch (error) {
    return null;
  }
}

// Extract enhanced IP data using current dual IP logic (from track.js)
function extractEnhancedIPData(originalConversion) {
  const extractedIPs = {
    primary_ip: null,
    conversion_ip: null,
    pageview_ip: null,
    all_ips: [],
    extraction_methods: {}
  };
  
  try {
    // Apply current enhanced IP extraction logic (from track.js)
    
    // Method 1: Primary IP (top-level)
    const primaryIP = originalConversion.ip_address || originalConversion.ip;
    if (primaryIP) {
      extractedIPs.primary_ip = primaryIP;
      extractedIPs.extraction_methods.PIP = 'top_level_ip';
    }
    
    // Method 2: Deep nested conversion IP (current track.js logic)
    let conversionIP = null;
    if (originalConversion.checkoutview?.pageviewcheckout?.pageview?.ip) {
      conversionIP = originalConversion.checkoutview.pageviewcheckout.pageview.ip;
      extractedIPs.extraction_methods.CIP = 'july_1st_deep_nested';
    } else if (originalConversion.customer?.ip_address) {
      conversionIP = originalConversion.customer.ip_address;
      extractedIPs.extraction_methods.CIP = 'june_28th_customer_nested';
    } else if (originalConversion.ip) {
      conversionIP = originalConversion.ip;
      extractedIPs.extraction_methods.CIP = 'top_level_fallback';
    }
    
    if (conversionIP) {
      extractedIPs.conversion_ip = conversionIP;
    }
    
    // Method 3: Pageview IP (fallback to conversion IP)
    extractedIPs.pageview_ip = conversionIP || primaryIP;
    extractedIPs.extraction_methods.IP = 'same_as_cip';
    
    // Collect all unique IPs
    const allIPs = [extractedIPs.primary_ip, extractedIPs.conversion_ip, extractedIPs.pageview_ip]
      .filter(Boolean)
      .filter((ip, index, arr) => arr.indexOf(ip) === index);
    
    extractedIPs.all_ips = allIPs;
    extractedIPs.dual_ip_detected = allIPs.length > 1;
    
    console.log(`🔧 Enhanced IP extraction for order ${originalConversion.order_id}:`, {
      primary_ip: extractedIPs.primary_ip,
      conversion_ip: extractedIPs.conversion_ip,
      dual_ip: extractedIPs.dual_ip_detected,
      ip_count: allIPs.length
    });
    
    return extractedIPs;
    
  } catch (extractionError) {
    console.warn('⚠️ Enhanced IP extraction error:', extractionError.message);
    return extractedIPs;
  }
}

// Process enhanced attribution recovery
async function processEnhancedAttributionRecovery(redis, conversionsWithData, extendedWindowHours, batchSize, maxTime) {
  console.log(`🔄 Starting enhanced attribution recovery for ${conversionsWithData.length} conversions...`);
  
  const processStartTime = Date.now();
  let recoveryAttempts = 0;
  let successfulRecoveries = 0;
  let additionalPageviewsFound = 0;
  let journeysRemaining = conversionsWithData.length;
  const recoveryDetails = [];
  
  // Process conversions in batches
  for (let i = 0; i < conversionsWithData.length; i += batchSize) {
    // Check timeout
    const timeRemaining = maxTime - (Date.now() - processStartTime);
    if (timeRemaining < 5000) {
      console.log(`⏰ Time limit reached after processing ${recoveryAttempts} recovery attempts`);
      break;
    }
    
    const batch = conversionsWithData.slice(i, i + batchSize);
    console.log(`🔄 Processing recovery batch ${Math.floor(i/batchSize) + 1}: ${i + 1}-${i + batch.length} of ${conversionsWithData.length}`);
    
    // Process this batch
    const batchResults = await processBatchRecovery(redis, batch, extendedWindowHours);
    
    recoveryAttempts += batch.length;
    successfulRecoveries += batchResults.successful_recoveries;
    additionalPageviewsFound += batchResults.additional_pageviews_found;
    journeysRemaining = conversionsWithData.length - (i + batch.length);
    recoveryDetails.push(...batchResults.recovery_details);
    
    console.log(`✅ Batch recovery complete: ${batchResults.successful_recoveries}/${batch.length} successful (${recoveryAttempts}/${conversionsWithData.length} total)`);
  }
  
  console.log(`🏁 Recovery processing summary: ${successfulRecoveries}/${recoveryAttempts} conversions recovered`);
  
  return {
    recovery_attempts: recoveryAttempts,
    successful_recoveries: successfulRecoveries,
    additional_pageviews_found: additionalPageviewsFound,
    journeys_remaining: journeysRemaining,
    recovery_details: recoveryDetails,
    processing_time_ms: Date.now() - processStartTime
  };
}

// Process batch recovery with enhanced attribution
async function processBatchRecovery(redis, batch, extendedWindowHours) {
  let successfulRecoveries = 0;
  let additionalPageviewsFound = 0;
  const recoveryDetails = [];
  
  const batchPromises = batch.map(async (conversionData) => {
    try {
      const recoveryStartTime = Date.now();
      
      // Apply enhanced attribution recovery
      const recoveredPageviews = await performEnhancedAttributionRecovery(redis, {
        conversion_timestamp: conversionData.conversion_timestamp,
        enhanced_ips: conversionData.enhanced_ip_extraction.all_ips,
        session_id: conversionData.original_conversion_data.session_id,
        device_signature: conversionData.original_conversion_data.device_signature || conversionData.original_conversion_data.dsig,
        screen_value: conversionData.original_conversion_data.screen_value || conversionData.original_conversion_data.SVV,
        gpu_signature: conversionData.original_conversion_data.gpu_signature || conversionData.original_conversion_data.gsig,
        geographic_data: conversionData.geographic_data,
        window_hours: extendedWindowHours
      });
      
      if (recoveredPageviews && recoveredPageviews.length > 0) {
        // Update existing journey with recovered pageviews
        await updateJourneyWithRecoveredPageviews(redis, conversionData, recoveredPageviews);
        
        successfulRecoveries++;
        additionalPageviewsFound += recoveredPageviews.length;
        
        recoveryDetails.push({
          journey_id: conversionData.journey_id,
          order_id: conversionData.conversion_order_id,
          customer_email: conversionData.customer_email,
          pageviews_recovered: recoveredPageviews.length,
          recovery_method: 'enhanced_dual_ip_extraction',
          recovery_time_ms: Date.now() - recoveryStartTime,
          attribution_methods: recoveredPageviews.map(pv => pv.attribution_method)
        });
        
        console.log(`✅ Recovery success: Order ${conversionData.conversion_order_id} - found ${recoveredPageviews.length} pageviews`);
      }
      
      return { success: recoveredPageviews.length > 0, pageviews: recoveredPageviews.length };
      
    } catch (recoveryError) {
      console.warn(`⚠️ Recovery error for order ${conversionData.conversion_order_id}:`, recoveryError.message);
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

// Perform enhanced attribution recovery (embedded logic from build-customer-journeys.js with enhancements)
async function performEnhancedAttributionRecovery(redis, params) {
  const { conversion_timestamp, enhanced_ips, session_id, device_signature, screen_value, gpu_signature, geographic_data, window_hours } = params;
  
  const conversionTime = new Date(conversion_timestamp).getTime();
  const windowStart = conversionTime - (window_hours * 60 * 60 * 1000);
  
  let allMatches = [];
  
  try {
    console.log(`🔍 Enhanced recovery search: ${enhanced_ips.length} IPs, ${window_hours}h window`);
    
    // PRIORITY 1: Enhanced IP Index Multi-Signal Search with ALL extracted IPs
    if (enhanced_ips && enhanced_ips.length > 0) {
      const ipMatches = await searchByEnhancedIPIndexes(redis, enhanced_ips, session_id, device_signature, screen_value, gpu_signature, windowStart, conversionTime);
      if (ipMatches.length > 0) {
        allMatches = allMatches.concat(ipMatches);
        console.log(`🎯 Enhanced IP search: found ${ipMatches.length} matches`);
        
        // Continue searching even with high-confidence matches to get complete picture
      }
    }
    
    // PRIORITY 2: Geographic Correlation (NEW - uses existing geographic keys)
    if (geographic_data && geographic_data.city && geographic_data.isp && allMatches.length < 5) {
      const geoMatches = await searchByGeographicCorrelation(redis, geographic_data, windowStart, conversionTime);
      if (geoMatches.length > 0) {
        geoMatches.forEach(match => {
          match.attribution_method = 'geographic_correlation';
          match.confidence = 150; // Lower confidence for geographic correlation
        });
        allMatches = allMatches.concat(geoMatches);
        console.log(`🌍 Geographic correlation: found ${geoMatches.length} matches`);
      }
    }
    
    // FALLBACK METHODS: Only if minimal matches found
    if (allMatches.length < 2) {
      
      // PRIORITY 3: Session ID Match (direct lookup)
      if (session_id) {
        const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
        if (sessionMatches.length > 0) {
          sessionMatches.forEach(match => {
            match.attribution_method = 'session_id_recovery';
            match.confidence = 300;
          });
          allMatches = allMatches.concat(sessionMatches);
        }
      }
      
      // PRIORITY 4: Device Signature Match (direct lookup)
      if (device_signature && allMatches.length < 3) {
        const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
        if (deviceMatches.length > 0) {
          deviceMatches.forEach(match => {
            match.attribution_method = 'device_signature_recovery';
            match.confidence = 260;
          });
          allMatches = allMatches.concat(deviceMatches);
        }
      }
    }
    
    // Remove duplicates and sort by confidence
    const uniqueMatches = removeDuplicateMatches(allMatches);
    uniqueMatches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
    
    console.log(`✅ Enhanced recovery complete: ${uniqueMatches.length} unique pageviews found`);
    return uniqueMatches;
    
  } catch (error) {
    console.warn('⚠️ Enhanced attribution recovery error:', error.message);
    return [];
  }
}

// NEW: Geographic correlation search using existing attribution_geo_* keys
async function searchByGeographicCorrelation(redis, geoData, windowStart, conversionTime) {
  const matches = [];
  
  try {
    const cleanCity = cleanForRedisKey(geoData.city);
    const cleanISP = cleanForRedisKey(geoData.isp?.substring(0, 20));
    
    if (cleanCity.length > 2 && cleanISP.length > 2) {
      // Search for geographic correlation keys
      let cursor = '0';
      let iterations = 0;
      const maxIterations = 3;
      
      do {
        const scanResult = await redis(`scan/${cursor}/match/attribution_geo_${cleanCity}_${cleanISP}_*/count/100`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        iterations++;
        
        // Check each geographic correlation key
        for (const key of keys) {
          try {
            const geoResult = await redis(`get/${key}`);
            if (geoResult?.result) {
              const pageview = JSON.parse(decodeURIComponent(geoResult.result));
              const pageviewTime = new Date(pageview.timestamp).getTime();
              
              if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
                matches.push({
                  ...pageview,
                  geographic_correlation: true,
                  matched_city: geoData.city,
                  matched_isp: geoData.isp
                });
              }
            }
          } catch (e) {
            // Skip invalid data
          }
        }
        
      } while (cursor !== '0' && iterations < maxIterations);
    }
    
  } catch (error) {
    console.warn('⚠️ Geographic correlation search failed:', error.message);
  }
  
  return matches;
}

// Update existing journey with recovered pageviews
async function updateJourneyWithRecoveredPageviews(redis, conversionData, recoveredPageviews) {
  try {
    // Load existing journey
    const existingJourneyData = await redis(`get/${conversionData.journey_key}`);
    if (!existingJourneyData?.result) {
      throw new Error('Existing journey not found');
    }
    
    const existingJourney = JSON.parse(decodeURIComponent(existingJourneyData.result));
    
    // Build enhanced journey with recovered pageviews
    const enhancedJourney = buildEnhancedJourneyFromRecovery(existingJourney, recoveredPageviews);
    
    // Update the journey record
    await redis(`setex/${conversionData.journey_key}/2592000/${encodeURIComponent(JSON.stringify(enhancedJourney))}`); // 30-day TTL
    
    console.log(`💾 Updated journey ${conversionData.journey_id} with ${recoveredPageviews.length} recovered pageviews`);
    
  } catch (updateError) {
    console.error(`❌ Error updating journey ${conversionData.journey_id}:`, updateError.message);
    throw updateError;
  }
}

// Build enhanced journey from recovery
function buildEnhancedJourneyFromRecovery(existingJourney, recoveredPageviews) {
  // Sort recovered pageviews by timestamp
  const sortedPageviews = recoveredPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Create new touchpoints from recovered pageviews
  const recoveredTouchpoints = sortedPageviews.map((pageview, index) => ({
    touchpoint_id: `${existingJourney.conversion_order_id}_recovered_${index + 1}`,
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
    recovery_method: 'enhanced_dual_ip_extraction',
    
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
  
  // Combine with existing conversion touchpoint
  const existingConversionTouchpoint = existingJourney.touchpoints.find(tp => tp.is_conversion || tp.type === 'conversion');
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
    recovery_method: 'enhanced_dual_ip_extraction',
    recovered_pageviews: sortedPageviews.length,
    reconstruction_method: 'enhanced_attribution_recovery'
  };
}

// Helper functions (embedded from existing scripts)

// Clean string for Redis key usage
function cleanForRedisKey(str) {
  if (!str || str === 'Unknown' || str === 'unknown') return '';
  return str.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
}

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
          let attributionMethod = 'ip_index_recovery';
          
          // PRIORITY 1: Session ID match (highest confidence)
          if (sessionId && pv.session_id === sessionId) {
            confidence = 295;
            attributionMethod = 'session_id_match_ip_index_recovery';
          }
          // PRIORITY 2: Device signature match
          else if (deviceSignature && pv.canvas_fingerprint === deviceSignature) {
            confidence = 255;
            attributionMethod = 'device_signature_match_ip_index_recovery';
          }
          // PRIORITY 3: Screen signature match
          else if (screenValue && pv.screen_resolution && hashString(pv.screen_resolution) === screenValue) {
            confidence = 195;
            attributionMethod = 'screen_signature_match_ip_index_recovery';
          }
          // PRIORITY 4: GPU signature match
          else if (gpuSignature && pv.webgl_fingerprint && hashString(pv.webgl_fingerprint) === gpuSignature) {
            confidence = 175;
            attributionMethod = 'webgl_signature_match_ip_index_recovery';
          }
          
          matches.push({
            ...pv,
            matched_ip: ip,
            match_method: 'enhanced_ip_index_recovery',
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
