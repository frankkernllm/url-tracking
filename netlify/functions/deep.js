exports.handler = async (event, context) => {
    // DEEP DIVE ATTRIBUTION SYSTEM v2.0: 8-Tier with 24-Hour Window
    // Processes ONE unattributed conversion per run using enhanced 8-tier system with 24-hour geographic correlation
    
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    try {
        console.log('🔍 Starting DEEP DIVE ATTRIBUTION SYSTEM v2.0 - 8-Tier with 24-Hour Window');
        console.log('⚡ Enhanced: 8-tier priority system with 24-hour geographic correlation fallback');
        
        const startTime = Date.now();
        
        // Global caching for this deep dive run
        let geoDataCache = new Map();
        let cacheStats = { hits: 0, misses: 0, api_calls: 0, redis_hits: 0 };
        
        // Step 1: Fetch analytics data from past 7 days
        const analyticsData = await fetchAnalyticsDataPast7Days();
        
        // Step 2: Find all conversions and filter for unattributed only (past 7 days)
        const allConversions = getAllConversions(analyticsData.conversions);
        const unattributedConversions = getUnattributedConversions(allConversions);
        
        // Step 3: Filter out conversions already processed with deep2 system
        const unprocessedDeep2Conversions = await filterNonDeep2Conversions(unattributedConversions);
        
        if (unprocessedDeep2Conversions.length === 0) {
            if (unattributedConversions.length > 0) {
                return {
                    statusCode: 200,
                    headers,
                    body: JSON.stringify({
                        success: true,
                        message: `🎯 All ${unattributedConversions.length} unattributed conversions (past 7 days) have been processed with deep2 system. No more to process.`,
                        results: { 
                            total_conversions: allConversions.length,
                            unattributed: unattributedConversions.length,
                            unprocessed_deep2: 0,
                            processed_this_run: 0,
                            status: 'ALL_DEEP2_PROCESSED'
                        }
                    })
                };
            } else {
                return {
                    statusCode: 200,
                    headers,
                    body: JSON.stringify({
                        success: true,
                        message: '🎉 All conversions (past 7 days) have attribution! No unattributed conversions found.',
                        results: { 
                            total_conversions: allConversions.length,
                            unattributed: 0,
                            unprocessed_deep2: 0,
                            processed_this_run: 0,
                            status: 'ALL_ATTRIBUTED'
                        }
                    })
                };
            }
        }
        
        console.log(`📋 Found ${unattributedConversions.length} unattributed conversions (past 7 days)`);
        console.log(`🎯 Found ${unprocessedDeep2Conversions.length} not yet processed with deep2 system`);
        console.log(`🔍 Processing the first unprocessed conversion with enhanced 8-tier system...`);
        
        // Step 4: Process the FIRST unprocessed conversion
        const conversionToProcess = unprocessedDeep2Conversions[0];
        
        console.log(`\n🔬 DEEP DIVE ANALYSIS v2.0: [PRIVACY PROTECTED]`);
        console.log(`   📍 IP: ${conversionToProcess.ip_address}`);
        console.log(`   ⏰ Time: ${conversionToProcess.timestamp}`);
        console.log(`   📊 Current: NO ATTRIBUTION`);
        console.log(`   🔍 Using enhanced 8-tier system with 24-hour window for Priority 8`);
        
        // Extract parameters with backwards compatibility
        const conversionData = extractConversionParameters(conversionToProcess);
        
        console.log(`   📊 Type: ${conversionData.has_enhanced_params ? 'ENHANCED' : 'LEGACY'} (age: ${conversionData.conversion_age_days} days)`);
        console.log(`   📍 IPs: PIP=${conversionData.PIP || 'none'}, CIP=${conversionData.CIP || 'none'}, IP=${conversionData.IP || 'none'}`);
        console.log(`   🔐 Signatures: SSID=${!!conversionData.SSID}, dsig=${!!conversionData.dsig}, SVV=${!!conversionData.SVV}`);
        
        // Step 5: Enhanced attribution analysis with 24-hour window
        const improvementResults = await analyzeConversionForAttributionDeep2(
            conversionToProcess, 
            analyticsData.page_views, 
            conversionData,
            geoDataCache,
            cacheStats
        );
        
        // Step 6: Update Redis if needed
        let updateResult = null;
        if (improvementResults.shouldUpdate) {
            console.log(`📝 Updating attribution for conversion...`);
            try {
                updateResult = await updateConversionAttributionDeep2(conversionToProcess, improvementResults);
            } catch (redisError) {
                console.error('❌ Redis update failed:', redisError);
            }
        }
        
        // Step 7: Mark as processed with deep2 flag
        await markConversionAsDeep2(conversionToProcess, improvementResults.attributionMethod);
        
        // Step 8: Generate response
        const remainingUnattributed = unattributedConversions.length - 1;
        const remainingUnprocessedDeep2 = unprocessedDeep2Conversions.length - 1;
        const wasSuccessful = improvementResults.matchFound;
        const processingTime = Date.now() - startTime;
        
        let summaryMessage;
        if (wasSuccessful) {
            summaryMessage = `✅ Found attribution using ${improvementResults.attributionMethod} (Priority ${improvementResults.priorityLevel})! ${remainingUnprocessedDeep2} unattributed conversions remaining.`;
        } else {
            summaryMessage = `❌ No attribution found even with enhanced 8-tier system + 24-hour window. ${remainingUnprocessedDeep2} unattributed conversions remaining.`;
        }
        
        console.log(`\n🏁 DEEP DIVE v2.0 COMPLETE:`);
        console.log(`   📧 Processed: [PRIVACY PROTECTED]`);
        console.log(`   ✅ Success: ${wasSuccessful ? 'YES' : 'NO'}`);
        console.log(`   🎯 Method: ${improvementResults.attributionMethod} (Priority ${improvementResults.priorityLevel || 'N/A'})`);
        console.log(`   🔄 Remaining unprocessed (deep2): ${remainingUnprocessedDeep2}`);
        console.log(`   ⏱️  Processing time: ${processingTime/1000}s`);
        console.log(`   📊 Cache: ${cacheStats.hits}H/${cacheStats.misses}M/${cacheStats.api_calls}A/${cacheStats.redis_hits}R`);
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                processed_conversion: {
                    email: '[PRIVACY_PROTECTED]',
                    timestamp: conversionToProcess.timestamp,
                    match_found: wasSuccessful,
                    attribution_method: improvementResults.attributionMethod,
                    priority_level: improvementResults.priorityLevel || null,
                    improvement_type: improvementResults.improvementType,
                    new_attribution: improvementResults.newAttribution,
                    confidence: improvementResults.match?.confidence || null,
                    time_difference_minutes: improvementResults.match?.timeDiff || null,
                    processing_time_ms: processingTime,
                    update_result: updateResult
                },
                message: summaryMessage,
                progress: {
                    total_conversions: allConversions.length,
                    unattributed_total: unattributedConversions.length,
                    unattributed_remaining_deep2: remainingUnprocessedDeep2,
                    processed_this_run: 1,
                    status: remainingUnprocessedDeep2 > 0 ? 'MORE_TO_PROCESS' : 'ALL_DEEP2_PROCESSED',
                    next_action: remainingUnprocessedDeep2 > 0 ? 'Press button again to process next conversion' : 'All unattributed conversions have been processed with deep2 system'
                },
                analysis: improvementResults.analysis,
                performance: {
                    processing_time_seconds: processingTime / 1000,
                    cache_performance: {
                        cache_hits: cacheStats.hits,
                        cache_misses: cacheStats.misses,
                        redis_hits: cacheStats.redis_hits,
                        api_calls: cacheStats.api_calls,
                        efficiency_percent: Math.round(cacheStats.hits / (cacheStats.hits + cacheStats.misses + 1) * 100)
                    }
                },
                enhancements: {
                    version: '2.0',
                    system: 'deep2_8tier_24hour',
                    features: ['8_tier_priority_system', 'backwards_compatibility', 'aggressive_caching', '24_hour_window']
                },
                processing_method: 'deep2_enhanced_8tier'
            })
        };

    } catch (error) {
        console.error('❌ Deep dive attribution system v2.0 error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Deep dive attribution system v2.0 failed',
                details: error.message,
                version: '2.0'
            })
        };
    }
};

// Backwards compatible data extraction (same as enhanced batch script)
function extractConversionParameters(conversion) {
    const conversionAge = Math.floor((Date.now() - new Date(conversion.timestamp)) / (1000 * 60 * 60 * 24));
    
    return {
        email: conversion.email,
        timestamp: conversion.timestamp,
        
        // Enhanced parameters (may not exist in older conversions)
        SSID: conversion.session_id || conversion.SSID || null,
        
        // IP precedence: newer conversions may have multiple IPs
        PIP: conversion.primary_ip || conversion.custom_ipv6 || null,
        CIP: conversion.conversion_ip || conversion.custom_ipv4 || null, 
        IP: conversion.ip_address || conversion.ip || null,
        
        // Device signatures (likely missing in older conversions)
        dsig: conversion.device_signature || 
              (conversion.canvas_fingerprint?.slice(-20)) || 
              null,
        SVV: conversion.screen_hash || 
             (conversion.screen_resolution ? hashString(conversion.screen_resolution) : null),
        gsig: conversion.webgl_hash || 
              (conversion.webgl_fingerprint ? hashString(conversion.webgl_fingerprint) : null),
        
        // Standard fields (should exist in most conversions)
        landing_page: conversion.landing_page,
        utm_source: conversion.utm_source || conversion.source,
        utm_campaign: conversion.utm_campaign || conversion.campaign,
        utm_medium: conversion.utm_medium,
        utm_content: conversion.utm_content,
        utm_term: conversion.utm_term,
        
        // Metadata for processing decisions
        has_enhanced_params: !!(conversion.session_id || conversion.device_signature || conversion.primary_ip),
        conversion_age_days: conversionAge,
        
        // Processing hints for deep dive
        requires_geo_correlation: !(conversion.session_id || conversion.device_signature),
        is_legacy_conversion: conversionAge > 30,
        processing_type: 'deep_dive_24h'
    };
}

// Hash function for privacy-safe parameter values (same as enhanced batch script)
function hashString(str) {
    if (!str) return null;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36); // Convert to base36 for shorter string
}

// Enhanced attribution analysis with 24-hour window (adapted from enhanced batch script)
async function analyzeConversionForAttributionDeep2(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('   🔬 Using enhanced 8-tier attribution system with 24-hour window...');
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: null, // Unattributed conversions have no attribution
        matchFound: false,
        newAttribution: null,
        shouldUpdate: false,
        improvementType: 'NO_CHANGE',
        attributionMethod: 'none',
        priorityLevel: 9,
        match: null,
        analysis: {
            conversion_type: conversionData.has_enhanced_params ? 'enhanced' : 'legacy',
            conversion_age_days: conversionData.conversion_age_days,
            processing_path: 'unknown',
            window_size: '24_hours'
        }
    };
    
    // Try enhanced 8-tier system first (Priorities 1-7: direct Redis lookups)
    if (conversionData.has_enhanced_params) {
        console.log('   🚀 Trying enhanced 8-tier attribution system (Priorities 1-7)...');
        results.analysis.processing_path = '8_tier_direct_lookups';
        
        const enhancedResult = await findEnhancedAttribution(conversionData);
        
        if (enhancedResult) {
            results.matchFound = true;
            results.newAttribution = enhancedResult.landing_page || enhancedResult.url;
            results.match = enhancedResult;
            results.attributionMethod = enhancedResult.method;
            results.priorityLevel = getPriorityLevel(enhancedResult.method);
            results.shouldUpdate = true; // Always update unattributed conversions
            results.improvementType = 'NEW_ATTRIBUTION_DIRECT';
            
            console.log(`   ✅ Direct match found: ${enhancedResult.method} (Priority ${results.priorityLevel})`);
            return results;
        }
    }
    
    // Priority 8: Geographic correlation with 24-hour window (main method for unattributed)
    console.log('   🌍 Priority 8: Geographic correlation with 24-hour window...');
    results.analysis.processing_path = 'geographic_correlation_24h';
    
    const geoResult = await performGeographicCorrelation24Hour(conversion, pageviews, conversionData, geoDataCache, cacheStats);
    
    if (geoResult) {
        results.matchFound = true;
        results.newAttribution = geoResult.newAttribution;
        results.match = geoResult.match;
        results.attributionMethod = geoResult.method || 'geo_correlation_24h';
        results.priorityLevel = 8;
        results.shouldUpdate = true; // Always update unattributed conversions
        results.improvementType = 'NEW_ATTRIBUTION_24H';
        
        console.log(`   ✅ 24-hour geographic correlation successful: ${results.attributionMethod}`);
    } else {
        console.log('   ❌ No attribution found via any method (including 24-hour window)');
        results.improvementType = 'NO_MATCH_FOUND_24H';
    }
    
    return results;
}

// 8-tier enhanced attribution system (same as enhanced batch script)
async function findEnhancedAttribution(conversionData) {
    console.log('      🎯 Starting 8-tier attribution search...');
    
    // Priority 1: Session ID Match (300 points) - HIGHEST PRIORITY
    if (conversionData.SSID) {
        console.log('      🔍 Priority 1: Trying SSID match:', conversionData.SSID);
        const sessionKey = `attribution_session_${conversionData.SSID}`;
        const sessionResult = await redisRequest('get', sessionKey);
        
        if (sessionResult) {
            const mainKey = sessionResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 1: SSID match found - highest confidence');
                return {
                    method: 'ssid_direct_match',
                    score: 300,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 1: SSID lookup failed');
    }
    
    // Priority 2: Primary IP Match (280 points) - IPv6 original or explicit primary IP
    if (conversionData.PIP) {
        console.log('      🔍 Priority 2: Trying Primary IP match:', conversionData.PIP);
        const pipKey = `attribution_ip_${encodeIPForKey(conversionData.PIP)}`;
        const pipResult = await redisRequest('get', pipKey);
        
        if (pipResult) {
            const mainKey = pipResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 2: Primary IP match found');
                return {
                    method: 'primary_ip_match',
                    score: 280,
                    matched_ip: 'primary',
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 2: Primary IP lookup failed');
    }
    
    // Priority 3: Conversion IP Match (260 points) - Top-level checkout IP
    if (conversionData.CIP && conversionData.CIP !== conversionData.PIP) {
        console.log('      🔍 Priority 3: Trying Conversion IP match:', conversionData.CIP);
        const cipKey = `attribution_ip_${encodeIPForKey(conversionData.CIP)}`;
        const cipResult = await redisRequest('get', cipKey);
        
        if (cipResult) {
            const mainKey = cipResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 3: Conversion IP match found');
                return {
                    method: 'conversion_ip_match',
                    score: 260,
                    matched_ip: 'conversion',
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 3: Conversion IP lookup failed');
    }
    
    // Priority 4: Pageview IP Match (240 points) - Nested pageview IP (backward compatibility)
    if (conversionData.IP && conversionData.IP !== conversionData.CIP && conversionData.IP !== conversionData.PIP) {
        console.log('      🔍 Priority 4: Trying Pageview IP match:', conversionData.IP);
        const ipKey = `attribution_ip_${encodeIPForKey(conversionData.IP)}`;
        const ipResult = await redisRequest('get', ipKey);
        
        if (ipResult) {
            const mainKey = ipResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 4: Pageview IP match found');
                return {
                    method: 'pageview_ip_match',
                    score: 240,
                    matched_ip: 'pageview',
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 4: Pageview IP lookup failed');
    }
    
    // Priority 5: Device Signature Match (220 points) - Cross-device attribution
    if (conversionData.dsig) {
        console.log('      🔍 Priority 5: Trying device signature match:', conversionData.dsig);
        const fpKey = `attribution_fp_${conversionData.dsig}`;
        const fpResult = await redisRequest('get', fpKey);
        
        if (fpResult) {
            const mainKey = fpResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 5: Device signature match found - cross-device attribution');
                return {
                    method: 'device_signature_match',
                    score: 220,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 5: Device signature lookup failed');
    }
    
    // Priority 6: Screen Hash Match (200 points) - Privacy-safe device correlation
    if (conversionData.SVV) {
        console.log('      🔍 Priority 6: Trying screen hash match:', conversionData.SVV);
        const screenKey = `attribution_screen_${conversionData.SVV}`;
        const screenResult = await redisRequest('get', screenKey);
        
        if (screenResult) {
            const mainKey = screenResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 6: Screen hash match found - privacy-safe device correlation');
                return {
                    method: 'screen_hash_match',
                    score: 200,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 6: Screen hash lookup failed');
    }
    
    // Priority 7: WebGL Signature Match (180 points) - Additional device validation
    if (conversionData.gsig) {
        console.log('      🔍 Priority 7: Trying WebGL signature match:', conversionData.gsig);
        const webglKey = `attribution_webgl_${conversionData.gsig}`;
        const webglResult = await redisRequest('get', webglKey);
        
        if (webglResult) {
            const mainKey = webglResult;
            const attributionResult = await redisRequest('get', mainKey);
            if (attributionResult) {
                const attrData = JSON.parse(attributionResult);
                console.log('      ✅ Priority 7: WebGL signature match found');
                return {
                    method: 'webgl_signature_match',
                    score: 180,
                    landing_page: attrData.landing_page,
                    ...attrData
                };
            }
        }
        console.log('      ⚠️ Priority 7: WebGL signature lookup failed');
    }
    
    console.log('      ❌ All 7 direct attribution methods failed');
    return null;
}

// Helper function to get priority level from method (same as enhanced batch script)
function getPriorityLevel(method) {
    const priorities = {
        'ssid_direct_match': 1,
        'primary_ip_match': 2,
        'conversion_ip_match': 3,
        'pageview_ip_match': 4,
        'device_signature_match': 5,
        'screen_hash_match': 6,
        'webgl_signature_match': 7,
        'geo_correlation_24h': 8,
        'geo_high_confidence_24h': 8,
        'geo_medium_confidence_24h': 8
    };
    return priorities[method] || 9;
}

// IPv6-safe key encoding (same as enhanced batch script)
function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
}

// Enhanced geographic correlation with 24-hour window and caching
async function performGeographicCorrelation24Hour(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('      🌍 Starting 24-hour cached geographic correlation...');
    
    // Get the best IP for correlation
    const testIPs = [conversionData.PIP, conversionData.CIP, conversionData.IP].filter(Boolean);
    
    if (testIPs.length === 0) {
        console.log('      ❌ No IPs available for geographic correlation');
        return null;
    }
    
    // Find pageviews in 24-hour window before conversion
    const candidatePageviews = findPageviewsIn24HourWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log('      ❌ No pageviews found in 24-hour window before conversion');
        return null;
    }
    
    console.log(`      📱 Found ${candidatePageviews.length} pageviews in 24-hour window`);
    
    // Test each customer IP for geographic correlation
    for (const customerIP of testIPs) {
        console.log(`      🌍 Testing geographic correlation for: ${customerIP}`);
        
        // Get geographic data for conversion IP (using cache)
        const conversionGeo = await getOrFetchGeoData(customerIP, geoDataCache, cacheStats);
        
        console.log(`      🌍 Conversion geo: ${conversionGeo.city}, ${conversionGeo.region} (${conversionGeo.isp})`);

        if (conversionGeo.city === 'LOOKUP_FAILED') {
            console.log('      ❌ Geographic lookup failed, trying next IP');
            continue;
        }

        // Find the best temporal match with geographic correlation (24-hour window)
        const bestMatch = await findBestTemporalMatch24Hour(conversion, candidatePageviews, conversionGeo, geoDataCache, cacheStats);
        
        if (bestMatch) {
            console.log(`      ✅ 24-hour geographic correlation successful: ${bestMatch.confidence} (score: ${Math.round(bestMatch.score)})`);
            
            return {
                newAttribution: bestMatch.pageview.landing_page || bestMatch.pageview.url,
                match: bestMatch,
                method: bestMatch.confidence === 'HIGH_CONFIDENCE' ? 'geo_high_confidence_24h' : 
                       bestMatch.confidence === 'MEDIUM_CONFIDENCE' ? 'geo_medium_confidence_24h' : 'geo_correlation_24h'
            };
        }
    }
    
    console.log('      ❌ 24-hour geographic correlation failed for all IPs');
    return null;
}

// Find pageviews in 24-hour window before conversion (ONLY CHANGE from 90-minute version)
function findPageviewsIn24HourWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 24 * 60 * 60 * 1000); // 24 hours before
    
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address; // Must have IP address
    });
    
    // Sort by timestamp DESCENDING (newest first = closest to conversion)
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return candidatePageviews;
}

// Find best temporal match with cached geographic data (24-hour version)
async function findBestTemporalMatch24Hour(conversion, candidatePageviews, conversionGeoData, geoDataCache, cacheStats) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        // Get pageview geographic data (using cache)
        const pageviewGeoData = await getOrFetchGeoData(pageview.ip_address, geoDataCache, cacheStats);
        
        // Compare geographic data with enhanced scoring
        const geoMatch = compareGeographicDataEnhanced(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`      🏆 TEMPORAL MATCH: ${timeDiff.toFixed(1)}min before (${geoMatch.confidence})`);
            
            return {
                pageview: pageview,
                score: geoMatch.score,
                timeDiff: timeDiff,
                confidence: geoMatch.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData,
                candidateNumber: i + 1
            };
        }
        
        // Progress logging for large datasets (more frequent for 24-hour window)
        if ((i + 1) % 100 === 0) {
            console.log(`      📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
            console.log(`      📞 API calls used: ${cacheStats.api_calls}/10`);
        }
        
        // Early termination if API limit reached
        if (cacheStats.api_calls >= 10) {
            console.log(`      ⚠️ API rate limit reached, stopping search at ${i + 1}/${candidatePageviews.length}`);
            break;
        }
    }
    
    return null;
}

// Enhanced cached geographic data strategy (same as enhanced batch script)
async function getCachedGeoData(ip, geoDataCache, cacheStats) {
    if (!ip || ip === 'unknown') return null;
    
    // Check in-memory cache first (for current run)
    if (geoDataCache.has(ip)) {
        cacheStats.hits++;
        console.log(`   💾 Using in-memory cache for ${ip}`);
        return geoDataCache.get(ip);
    }
    
    try {
        // Check Redis cache (from store-attribution.js and track.js)
        const encodedIP = ip.replace(/:/g, '_');
        const cacheKey = `geo_cache:${encodedIP}`;
        const cachedResult = await redisRequest('get', cacheKey);
        
        if (cachedResult) {
            const geoData = JSON.parse(decodeURIComponent(cachedResult));
            geoDataCache.set(ip, geoData); // Store in memory for this run
            cacheStats.redis_hits++;
            console.log(`   📦 Using Redis cache for ${ip}: ${geoData.city}, ${geoData.region}`);
            return geoData;
        }
        
        // Check if we already have geo data in existing attribution records
        const ipKey = `attribution_ip_${encodedIP}`;
        const attrKeyResult = await redisRequest('get', ipKey);
        
        if (attrKeyResult) {
            const attrResult = await redisRequest('get', attrKeyResult);
            if (attrResult) {
                const attrData = JSON.parse(attrResult);
                if (attrData.geographic_data) {
                    geoDataCache.set(ip, attrData.geographic_data);
                    cacheStats.redis_hits++;
                    console.log(`   🔗 Using geo data from attribution record for ${ip}`);
                    return attrData.geographic_data;
                }
            }
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        console.log(`   ⚠️ Cache lookup failed for ${ip}: ${error.message}`);
        cacheStats.misses++;
        return null;
    }
}

async function getOrFetchGeoData(ip, geoDataCache, cacheStats) {
    // Try cache first
    let geoData = await getCachedGeoData(ip, geoDataCache, cacheStats);
    if (geoData) return geoData;
    
    // Only make API call if absolutely necessary and within rate limits
    if (cacheStats.api_calls < 10) { // Limit API calls per deep dive run
        try {
            const ipinfoToken = process.env.IPINFO_TOKEN;
            if (!ipinfoToken) return getFailedLookupData(ip);
            
            console.log(`   🌍 Making IPinfo API call for ${ip} (${cacheStats.api_calls + 1}/10)`);
            const response = await fetch(`https://ipinfo.io/${ip}?token=${ipinfoToken}`, {
                signal: AbortSignal.timeout(3000)
            });
            
            if (response.ok) {
                const data = await response.json();
                geoData = {
                    ip: data.ip,
                    city: data.city || 'Unknown',
                    region: data.region || 'Unknown', 
                    country: data.country || 'Unknown',
                    isp: extractBestISP(data),
                    coordinates: data.loc || '0,0',
                    timezone: data.timezone || 'Unknown',
                    lookup_timestamp: new Date().toISOString()
                };
                
                // Cache in both memory and Redis
                geoDataCache.set(ip, geoData);
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                await redisRequest('setex', cacheKey, 86400, encodeURIComponent(JSON.stringify(geoData)));
                
                cacheStats.api_calls++;
                console.log(`   ✅ Cached geo data for ${ip}: ${geoData.city}, ${geoData.region} (${geoData.isp})`);
                return geoData;
            }
        } catch (error) {
            console.log(`   ❌ IPinfo API call failed for ${ip}: ${error.message}`);
        }
    } else {
        console.log(`   ⚠️ Skipping API call for ${ip} - rate limit reached (${cacheStats.api_calls}/10)`);
    }
    
    return getFailedLookupData(ip);
}

function getFailedLookupData(ip) {
    return {
        ip: ip || 'unknown',
        city: 'LOOKUP_FAILED',
        region: 'LOOKUP_FAILED', 
        country: 'LOOKUP_FAILED',
        isp: 'LOOKUP_FAILED',
        coordinates: '0,0',
        timezone: 'Unknown'
    };
}

function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

// Enhanced geographic scoring to match enhanced batch script
function compareGeographicDataEnhanced(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    
    // ISP + Location combinations (primary correlation method for dual-stack)
    if (conversionGeo.isp !== 'Unknown' && pageviewGeo.isp !== 'Unknown') {
        if (normalizeISP(conversionGeo.isp) === normalizeISP(pageviewGeo.isp)) {
            if (cityMatch) {
                score += 60; // High confidence: same city + ISP
            } else if (regionMatch) {
                score += 40; // Medium confidence: same region + ISP  
            } else if (countryMatch) {
                score += 20; // Low confidence: same country + ISP
            }
        }
    }

    // Geographic-only fallbacks (lower confidence)
    if (cityMatch && conversionGeo.city !== 'Unknown') {
        score += 20; // Same city bonus
    }
    if (regionMatch && conversionGeo.region !== 'Unknown') {
        score += 10; // Same region bonus
    }

    let confidence = 'NO_MATCH';
    let isMatch = false;

    // Enhanced thresholds for geographic correlation
    if (score >= 80) {
        confidence = 'HIGH_CONFIDENCE';
        isMatch = true;
    } else if (score >= 60) {
        confidence = 'MEDIUM_CONFIDENCE';
        isMatch = true;
    } else if (score >= 40) {
        confidence = 'LOW_CONFIDENCE';
        isMatch = true; // Accept lower threshold since it's final fallback
    }

    return {
        isMatch,
        confidence,
        score,
        cityMatch,
        regionMatch,
        countryMatch,
        ispMatch
    };
}

// Compare ISPs with normalization (same as enhanced batch script)
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    return normalizeISP(isp1) === normalizeISP(isp2);
}

// Normalize ISP names for better matching (same as enhanced batch script)
function normalizeISP(isp) {
    if (!isp || isp === 'Unknown') return '';
    
    const normalized = isp.toLowerCase().replace(/[^a-z0-9]/g, '');
    
    // Handle common ISP name variations
    if (normalized.includes('twc') || normalized.includes('timewarner') || normalized.includes('spectruminternet')) {
        return 'timewarner';
    }
    if (normalized.includes('comcast') || normalized.includes('xfinity')) {
        return 'comcast';
    }
    if (normalized.includes('verizon') || normalized.includes('vzw')) {
        return 'verizon';
    }
    
    return normalized;
}

// Enhanced attribution update for deep2 system
async function updateConversionAttributionDeep2(conversion, improvementResults) {
    try {
        // Find the conversion record in Redis
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            // Get existing data
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            // Update with deep2 attribution
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match.pageview?.source || improvementResults.match.source || 'deep2_enhanced',
                utm_campaign: improvementResults.match.pageview?.utm_campaign || improvementResults.match.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match.pageview?.utm_medium || improvementResults.match.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match.pageview?.referrer_url || improvementResults.match.referrer_url || conversionData.referrer_url,
                
                // Deep2 attribution improvement metadata
                attribution_improvement: {
                    method: improvementResults.attributionMethod,
                    improvement_type: improvementResults.improvementType,
                    priority_level: improvementResults.priorityLevel,
                    confidence: improvementResults.match.confidence || 'medium',
                    score: improvementResults.match.score || 0,
                    time_difference_minutes: improvementResults.match.timeDiff || 0,
                    improved_at: new Date().toISOString(),
                    system_version: '2.0',
                    processing_path: improvementResults.analysis?.processing_path || 'deep2_enhanced',
                    window_size: '24_hours',
                    
                    // Attribution source metadata
                    pageview_ip: improvementResults.match.pageview?.ip_address || improvementResults.match.ip_address,
                    pageview_timestamp: improvementResults.match.pageview?.timestamp || improvementResults.match.timestamp,
                    matched_ip_type: improvementResults.match.matched_ip || 'unknown'
                },
                
                // Store previous attribution (should be null for unattributed)
                previous_attribution: conversionData.landing_page || null,
                attributed_pageview_timestamp: improvementResults.match.pageview?.timestamp || improvementResults.match.timestamp || new Date().toISOString()
            };
            
            // Save back to Redis
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
                improvement_type: improvementResults.improvementType,
                attribution_method: improvementResults.attributionMethod,
                priority_level: improvementResults.priorityLevel
            };
            
        } else {
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// Mark conversion as processed with deep2 system
async function markConversionAsDeep2(conversion, attributionMethod) {
    try {
        const keyDeep2 = `deep2:${conversion.email}:${conversion.timestamp}`;
        const processedData = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'deep2_8tier_24hour',
            version: '2.0',
            attribution_method: attributionMethod,
            window_size: '24_hours',
            processing_type: 'single_conversion_deep_dive'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', keyDeep2, 2592000, JSON.stringify(processedData)); // 30 days
        console.log(`   ✅ Marked conversion as deep2 processed`);
    } catch (error) {
        console.log(`   ⚠️ Could not mark conversion as deep2: ${error.message}`);
    }
}

// Filter out conversions already processed with deep2 system
async function filterNonDeep2Conversions(unattributedConversions) {
    const unprocessedConversions = [];
    let alreadyProcessedDeep2Count = 0;
    
    for (const conversion of unattributedConversions) {
        const keyDeep2 = `deep2:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const processedData = await redisRequest('get', keyDeep2);
            
            if (processedData) {
                alreadyProcessedDeep2Count++;
                console.log(`   ⏭️ Skipping [PRIVACY PROTECTED] - already processed with deep2 system`);
            } else {
                unprocessedConversions.push(conversion);
            }
        } catch (error) {
            // If we can't check status, assume unprocessed
            unprocessedConversions.push(conversion);
        }
    }
    
    console.log(`📊 Filtered out ${alreadyProcessedDeep2Count} already processed with deep2 system`);
    return unprocessedConversions;
}

// EXISTING FUNCTIONS (maintained for compatibility)

// Fetch analytics data for past 7 days (same as enhanced batch script)
async function fetchAnalyticsDataPast7Days() {
    console.log('📊 Fetching analytics data for past 7 days...');
    
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 7);
    
    const earliestDate = new Date('2025-06-11');
    if (startDate < earliestDate) {
        startDate.setTime(earliestDate.getTime());
    }
    
    const startDateStr = startDate.toISOString().split('T')[0];
    const endDateStr = endDate.toISOString().split('T')[0];
    
    console.log(`📅 Safe date range: ${startDateStr} to ${endDateStr} (respecting data availability)`);
    
    const params = new URLSearchParams();
    params.append('start_date', startDateStr);
    params.append('end_date', endDateStr);
    
    const apiUrl = `https://trackingojoy.netlify.app/.netlify/functions/analytics?${params}`;
    
    const response = await fetch(apiUrl, {
        headers: {
            'X-API-Key': process.env.OJOY_API_KEY
        }
    });
    
    if (!response.ok) {
        throw new Error(`Analytics API failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    console.log(`✅ Analytics data loaded for ${startDateStr} to ${endDateStr}:`);
    console.log(`   📊 Total conversions: ${data.conversions?.length || 0}`);
    console.log(`   📊 Total pageviews: ${data.page_views?.length || 0}`);
    
    return data;
}

// Get ALL conversions (same as enhanced batch script)
function getAllConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    const sortedConversions = conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`📊 Found ${sortedConversions.length} total conversions for deep2 evaluation`);
    
    return sortedConversions;
}

// Get ONLY unattributed conversions (same as original deep dive script)
function getUnattributedConversions(allConversions) {
    const unattributed = allConversions.filter(conv => {
        const hasNoAttribution = !conv.landing_page || 
                                conv.landing_page === '' || 
                                conv.landing_page === 'NO ATTRIBUTION' ||
                                conv.landing_page === null ||
                                conv.landing_page === undefined;
        return hasNoAttribution;
    });
    
    console.log(`🔍 Found ${unattributed.length} unattributed conversions out of ${allConversions.length} total`);
    
    if (unattributed.length > 0) {
        console.log('📋 Unattributed conversions:');
        unattributed.slice(0, 5).forEach((conv, index) => {
            console.log(`   ${index + 1}. [PRIVACY PROTECTED] | ${conv.timestamp}`);
        });
        if (unattributed.length > 5) {
            console.log(`   ... and ${unattributed.length - 5} more`);
        }
    }
    
    return unattributed;
}

// Redis request helper (same as enhanced batch script)
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    let response;
    
    try {
        if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
            response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify([command, ...args])
            });
        } else {
            const encodedArgs = args.map(arg => encodeURIComponent(arg));
            const requestUrl = `${url}/${command}/${encodedArgs.join('/')}`;
            
            response = await fetch(requestUrl, {
                headers: {
                    'Authorization': `Bearer ${token}`,
                    'Content-Type': 'application/json'
                }
            });
        }
        
        if (!response.ok) {
            if (response.status === 404) {
                return null;
            }
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
        
    } catch (error) {
        throw error;
    }
}

// Find conversion key in Redis (same as enhanced batch script)
async function findConversionKey(conversion) {
    try {
        const patterns = [
            `conversions:*${conversion.email}*`,
            `conversions:${conversion.timestamp.split('T')[0]}*`,
            `conversions:*`,
            `conversion_${conversion.email}_*`,
            `conv_${conversion.email}_*`,
            `track_${conversion.email}_*`,
            `*${conversion.email}*`
        ];
        
        for (const pattern of patterns) {
            try {
                const keys = await redisRequest('keys', pattern);
                if (keys && keys.length > 0) {
                    for (const key of keys) {
                        const data = await redisRequest('get', key);
                        if (data) {
                            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
                            if (parsed.email === conversion.email && 
                                Math.abs(new Date(parsed.timestamp) - new Date(conversion.timestamp)) < 60000) {
                                return key;
                            }
                        }
                    }
                }
            } catch (error) {
                continue;
            }
        }
        
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for conversion:`, error);
        return null;
    }
}
