// FIXED: attribution-recovery-engine.js - Storage and Error Handling Fixes
// Key fixes: Fallback journey creation, proper Redis storage, enhanced error handling

// ... (keeping existing imports and setup) ...

// FIXED: Process batch recovery with proper fallback creation
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
        // FIXED: Try update first, fallback to create new journey
        const updateResult = await updateOrCreateRecoveredJourney(redis, conversionData, recoveredPageviews);
        
        successfulRecoveries++;
        additionalPageviewsFound += recoveredPageviews.length;
        
        recoveryDetails.push({
          journey_id: updateResult.journey_id,
          order_id: conversionData.conversion_order_id,
          customer_email: conversionData.customer_email,
          pageviews_recovered: recoveredPageviews.length,
          recovery_method: updateResult.method,
          recovery_time_ms: Date.now() - recoveryStartTime,
          attribution_methods: recoveredPageviews.map(pv => pv.attribution_method),
          storage_verified: updateResult.storage_verified
        });
        
        console.log(`✅ Recovery success: Order ${conversionData.conversion_order_id} - found ${recoveredPageviews.length} pageviews (${updateResult.method})`);
      } else {
        console.log(`❌ No recovery: Order ${conversionData.conversion_order_id} - no pageviews found in ${extendedWindowHours}h window`);
      }
      
      return { success: recoveredPageviews.length > 0, pageviews: recoveredPageviews.length };
      
    } catch (recoveryError) {
      console.error(`❌ Recovery error for order ${conversionData.conversion_order_id}:`, recoveryError.message);
      console.error(`❌ Recovery stack trace:`, recoveryError.stack);
      return { success: false, pageviews: 0, error: recoveryError.message };
    }
  });
  
  const batchResults = await Promise.all(batchPromises);
  
  return {
    successful_recoveries: successfulRecoveries,
    additional_pageviews_found: additionalPageviewsFound,
    recovery_details: recoveryDetails
  };
}

// FIXED: Update existing journey OR create new recovery journey with proper error handling
async function updateOrCreateRecoveredJourney(redis, conversionData, recoveredPageviews) {
  try {
    // STEP 1: Try to update existing journey
    const existingJourneyData = await redis(`get/${conversionData.journey_key}`, 2000);
    
    if (existingJourneyData?.result) {
      console.log(`🔄 Updating existing journey: ${conversionData.journey_key}`);
      
      const existingJourney = JSON.parse(decodeURIComponent(existingJourneyData.result));
      const enhancedJourney = buildEnhancedJourneyFromRecovery(existingJourney, recoveredPageviews);
      
      // Store updated journey with verification
      const updateSuccess = await storeJourneyWithVerification(redis, conversionData.journey_key, enhancedJourney);
      
      if (updateSuccess) {
        return {
          journey_id: enhancedJourney.journey_id,
          method: 'existing_journey_updated',
          storage_verified: true
        };
      }
    }
    
    // STEP 2: Fallback - Create new recovery journey
    console.log(`🆕 Creating new recovery journey for order ${conversionData.conversion_order_id}`);
    
    const newJourney = createNewRecoveryJourney(conversionData, recoveredPageviews);
    const newJourneyKey = `customer_journey:${newJourney.journey_id}`;
    
    // Store new journey with verification
    const createSuccess = await storeJourneyWithVerification(redis, newJourneyKey, newJourney);
    
    if (createSuccess) {
      console.log(`💾 Created recovery journey: ${newJourney.journey_id} with ${recoveredPageviews.length} pageviews`);
      return {
        journey_id: newJourney.journey_id,
        method: 'new_recovery_journey_created',
        storage_verified: true
      };
    } else {
      throw new Error('Failed to store new recovery journey');
    }
    
  } catch (error) {
    console.error(`❌ Critical error in updateOrCreateRecoveredJourney:`, error.message);
    console.error(`❌ Error details: order=${conversionData.conversion_order_id}, journey_key=${conversionData.journey_key}`);
    throw error;
  }
}

// FIXED: Store journey with verification and proper error handling
async function storeJourneyWithVerification(redis, journeyKey, journeyData) {
  try {
    console.log(`🔄 Attempting to store journey: ${journeyKey}`);
    console.log(`📦 Journey data size: ${JSON.stringify(journeyData).length} characters`);
    
    // STEP 1: Store with TTL
    const storeResult = await redis(`setex/${journeyKey}/2592000/${encodeURIComponent(JSON.stringify(journeyData))}`, 5000);
    console.log(`📋 Redis store result:`, JSON.stringify(storeResult));
    
    if (storeResult.result !== "OK") {
      console.error(`❌ Redis store failed: ${JSON.stringify(storeResult)}`);
      return false;
    }
    
    // STEP 2: Verify storage by reading back
    console.log(`🔍 Verifying storage with read-back test...`);
    const verifyResult = await redis(`get/${journeyKey}`, 3000);
    
    if (verifyResult?.result) {
      console.log(`✅ Storage verification successful for: ${journeyKey}`);
      return true;
    } else {
      console.error(`❌ Storage verification failed - data not found after write: ${journeyKey}`);
      return false;
    }
    
  } catch (storageError) {
    console.error(`❌ Storage error for ${journeyKey}:`, storageError.message);
    console.error(`❌ Storage stack trace:`, storageError.stack);
    return false;
  }
}

// FIXED: Create new recovery journey with proper data structure
function createNewRecoveryJourney(conversionData, recoveredPageviews) {
  // Sort recovered pageviews by timestamp
  const sortedPageviews = recoveredPageviews.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  // Create touchpoints from recovered pageviews
  const recoveredTouchpoints = sortedPageviews.map((pageview, index) => ({
    touchpoint_id: `${conversionData.conversion_order_id}_recovered_${index + 1}`,
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
  
  // Add conversion touchpoint
  const conversionTouchpoint = {
    touchpoint_id: `${conversionData.conversion_order_id}_conversion`,
    timestamp: conversionData.conversion_timestamp,
    type: 'conversion',
    order_id: conversionData.conversion_order_id,
    order_total: conversionData.conversion_value,
    email: conversionData.customer_email,
    attribution_method: 'conversion_point',
    confidence: 1000,
    touchpoint_position: recoveredTouchpoints.length + 1,
    is_conversion: true,
    is_last_touchpoint: true
  };
  
  const allTouchpoints = [...recoveredTouchpoints, conversionTouchpoint];
  
  // Calculate journey metrics
  const journeyStart = new Date(allTouchpoints[0].timestamp);
  const journeyEnd = new Date(conversionData.conversion_timestamp);
  const journeySpanHours = (journeyEnd - journeyStart) / (1000 * 60 * 60);
  
  const uniqueSessions = new Set(allTouchpoints.map(t => t.session_id).filter(Boolean)).size;
  const uniqueDeviceFingerprints = new Set(allTouchpoints.map(t => t.canvas_fingerprint).filter(Boolean)).size;
  const uniqueSources = new Set(allTouchpoints.map(t => t.source).filter(Boolean));
  
  // FIXED: Use consistent journey ID format and ensure all required fields
  const journeyId = `journey_${conversionData.conversion_order_id}_recovery_${Date.now()}`;
  
  return {
    journey_id: journeyId,
    customer_email: conversionData.customer_email,
    conversion_timestamp: conversionData.conversion_timestamp,
    conversion_order_id: conversionData.conversion_order_id, // ✅ CRITICAL: Ensure this field exists
    conversion_value: conversionData.conversion_value,
    
    journey_start: allTouchpoints[0].timestamp,
    journey_end: conversionData.conversion_timestamp,
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
    
    // Recovery metadata
    recovery_attempted: true,
    recovery_timestamp: new Date().toISOString(),
    recovery_method: 'enhanced_dual_ip_extraction',
    recovered_pageviews: sortedPageviews.length,
    reconstruction_method: 'attribution_recovery_engine',
    
    created_at: new Date().toISOString()
  };
}

// ... (rest of existing functions remain the same) ...
