exports.handler = async (event, context) => {
    // ENHANCED ATTRIBUTION IMPROVEMENT SYSTEM v2.1: Backwards Compatible 8-Tier System
    // Key Features: 1) 8-tier priority system, 2) Backwards compatibility, 3) Aggressive caching, 4) Smart batch sizing
    
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
        console.log('🔧 Starting ENHANCED ATTRIBUTION IMPROVEMENT SYSTEM v2.1 - Backwards Compatible');
        console.log('⚡ Features: 8-tier priority system, backwards compatibility, aggressive caching, smart batching');
        
        // Enhanced timeout protection with more conservative timing
        const startTime = Date.now();
        const maxRunTime = 25000; // Increased to 25 seconds due to enhanced processing
        let processedInThisRun = 0;
        const processedConversions = [];
        
        // Global caching for this batch run
        let geoDataCache = new Map();
        let cacheStats = { hits: 0, misses: 0, api_calls: 0, redis_hits: 0 };
        
        // Step 1: Fetch analytics data from past 7 days (safe date range)
        const analyticsData = await fetchAnalyticsDataPast7Days();
        
        // Step 2: Find all conversions (not just unattributed)
        const allConversions = getAllConversions(analyticsData.conversions);
        
        if (allConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No conversions found in past 7 days',
                    results: { total: 0, processed: 0, remaining: 0 }
                })
            };
        }
        
        console.log(`🔄 ENHANCED BATCH PROCESSING: Smart batch sizing (1-2 conversions) for max ${maxRunTime/1000} seconds`);
        
        // Step 3: Get initial list of non-process3 conversions (ONCE at start)
        console.log('🔍 Getting initial list of non-process3 conversions...');
        let unprocessedConversions = await filterNonProcess3Conversions(allConversions);
        
        if (unprocessedConversions.length === 0) {
            console.log('🎉 ALL CONVERSIONS ALREADY COMPLETED WITH ENHANCED SYSTEM!');
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'All conversions already completed with enhanced attribution',
                    progress: { total_conversions: allConversions.length, total_processed: allConversions.length, remaining_conversions: 0, status: 'ALL_COMPLETE' }
                })
            };
        }
        
        console.log(`📋 Found ${unprocessedConversions.length} non-process3 conversions (will process with smart batching)`);
        console.log(`📊 Estimated processing time: ${Math.ceil(unprocessedConversions.length * 8)} seconds`);
        
        // Main processing loop - SMART batch sizing
        while (unprocessedConversions.length > 0 && Date.now() - startTime < maxRunTime) {
            
            // Check if we have enough time for another batch
            const timeRemaining = maxRunTime - (Date.now() - startTime);
            if (timeRemaining < 8000) { // Increased buffer time
                console.log(`⏰ Approaching timeout: ${timeRemaining/1000}s remaining - stopping batch processing`);
                break;
            }
            
            // Smart batch sizing based on conversion complexity and remaining time
            let batchSize = 1; // Default to 1 for safety
            
            if (timeRemaining > 15000) {
                // If we have plenty of time, analyze next conversion for batch size decision
                const nextConversion = unprocessedConversions[0];
                const conversionData = extractConversionParameters(nextConversion);
                
                if (conversionData.has_enhanced_params || conversionData.conversion_age_days < 7) {
                    batchSize = 1; // Recent conversions with enhanced params need more processing
                } else {
                    batchSize = Math.min(2, unprocessedConversions.length); // Older conversions might be faster
                }
            }
            
            const currentBatch = unprocessedConversions.splice(0, batchSize);
            const batchStartTime = Date.now();
            
            console.log(`\n📦 PROCESSING BATCH: ${batchSize} conversions (${unprocessedConversions.length} remaining after this batch)`);
            
            // Reset cache stats for each batch
            const batchCacheStats = { hits: 0, misses: 0, api_calls: 0, redis_hits: 0 };
            
            // Process each conversion in the current batch
            for (let i = 0; i < currentBatch.length; i++) {
                const conversionToProcess = currentBatch[i];
                const conversionStartTime = Date.now();
                
                console.log(`\n🎯 CONVERSION ${i + 1}/${batchSize}: ${conversionToProcess.email}`);
                
                // Extract parameters with backwards compatibility
                const conversionData = extractConversionParameters(conversionToProcess);
                
                console.log(`   📊 Type: ${conversionData.has_enhanced_params ? 'ENHANCED' : 'LEGACY'} (age: ${conversionData.conversion_age_days} days)`);
                console.log(`   📍 IPs: PIP=${conversionData.PIP || 'none'}, CIP=${conversionData.CIP || 'none'}, IP=${conversionData.IP || 'none'}`);
                console.log(`   🔐 Signatures: SSID=${!!conversionData.SSID}, dsig=${!!conversionData.dsig}, SVV=${!!conversionData.SVV}`);
                console.log(`   📊 Current: ${conversionToProcess.landing_page || 'NONE'}`);
                
                // Step 5: Enhanced attribution analysis with backwards compatibility
                const improvementResults = await analyzeConversionForAttributionEnhanced(
                    conversionToProcess, 
                    analyticsData.page_views, 
                    conversionData,
                    geoDataCache,
                    batchCacheStats
                );
                
                // Step 6: Update Redis if needed
                let updateResult = null;
                if (improvementResults.shouldUpdate) {
                    console.log(`📝 Updating attribution for ${conversionToProcess.email}...`);
                    try {
                        updateResult = await updateConversionAttributionEnhanced(conversionToProcess, improvementResults);
                    } catch (redisError) {
                        console.error('❌ Redis update failed:', redisError);
                    }
                }
                
                // Step 7: Mark as process3 with enhanced tracking
                await markConversionAsProcess3Enhanced(conversionToProcess, improvementResults.attributionMethod);
                
                // Track this processed conversion
                const conversionTime = Date.now() - conversionStartTime;
                processedInThisRun++;
                processedConversions.push({
                    email: conversionToProcess.email,
                    improvement_type: improvementResults.improvementType,
                    attribution_method: improvementResults.attributionMethod,
                    priority_level: improvementResults.priorityLevel || 8,
                    processing_time_ms: conversionTime,
                    cache_performance: `${batchCacheStats.hits}H/${batchCacheStats.misses}M/${batchCacheStats.api_calls}A/${batchCacheStats.redis_hits}R`,
                    update_result: updateResult
                });
                
                // Update global cache stats
                cacheStats.hits += batchCacheStats.hits;
                cacheStats.misses += batchCacheStats.misses;
                cacheStats.api_calls += batchCacheStats.api_calls;
                cacheStats.redis_hits += batchCacheStats.redis_hits;
                
                console.log(`✅ Completed ${conversionToProcess.email} in ${conversionTime/1000}s`);
                console.log(`   📊 Cache: ${batchCacheStats.hits}H/${batchCacheStats.misses}M/${batchCacheStats.api_calls}A/${batchCacheStats.redis_hits}R`);
            }
            
            // Batch completion summary
            const batchTime = Date.now() - batchStartTime;
            const cacheEfficiency = batchCacheStats.hits + batchCacheStats.misses > 0 
                ? Math.round(batchCacheStats.hits / (batchCacheStats.hits + batchCacheStats.misses) * 100) 
                : 0;
            
            console.log(`📦 Batch complete: ${batchSize} conversions in ${batchTime/1000}s | Cache efficiency: ${cacheEfficiency}%`);
        }
        
        // Final status calculation
        const totalTime = Date.now() - startTime;
        const totalRemaining = unprocessedConversions.length;
        const totalProcessed = allConversions.length - totalRemaining;
        const isComplete = totalRemaining === 0;
        
        console.log(`\n🏁 ENHANCED RUN COMPLETE:`);
        console.log(`   ⏱️  Total run time: ${totalTime/1000}s`);
        console.log(`   ✅ Processed in this run: ${processedInThisRun}`);
        console.log(`   📊 Total processed overall: ${totalProcessed}/${allConversions.length}`);
        console.log(`   🔄 Remaining: ${totalRemaining}`);
        console.log(`   📊 Cache performance: ${cacheStats.hits}H/${cacheStats.misses}M/${cacheStats.api_calls}A/${cacheStats.redis_hits}R`);
        console.log(`   🎯 Status: ${isComplete ? 'ALL COMPLETE' : 'RUN AGAIN TO CONTINUE'}`);
        
        // Generate enhanced summary message
        let summaryMessage;
        if (isComplete) {
            summaryMessage = `🎉 ALL ${allConversions.length} CONVERSIONS COMPLETED! Enhanced attribution v2.1 processed ${processedInThisRun} conversions in final run.`;
        } else if (processedInThisRun > 0) {
            summaryMessage = `✅ Enhanced v2.1: Processed ${processedInThisRun} conversions (${totalRemaining} remaining). Cache efficiency: ${Math.round(cacheStats.hits / (cacheStats.hits + cacheStats.misses + 1) * 100)}%`;
        } else {
            summaryMessage = `⚠️ No conversions processed (may be near timeout limit). ${totalRemaining} conversions remaining.`;
        }
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                batch_complete: true,
                processed_this_run: processedInThisRun,
                processed_conversions: processedConversions,
                message: summaryMessage,
                progress: {
                    total_conversions: allConversions.length,
                    total_processed: totalProcessed,
                    remaining_conversions: totalRemaining,
                    processed_this_batch: processedInThisRun,
                    status: isComplete ? 'ALL_COMPLETE' : 'CONTINUE',
                    next_action: isComplete ? 'All conversions optimized with enhanced attribution!' : 'Run the function again to continue processing'
                },
                performance: {
                    total_run_time_seconds: totalTime / 1000,
                    average_time_per_conversion: processedInThisRun > 0 ? (totalTime / processedInThisRun / 1000) : 0,
                    timeout_limit_seconds: maxRunTime / 1000,
                    cache_performance: {
                        total_lookups: cacheStats.hits + cacheStats.misses,
                        cache_hits: cacheStats.hits,
                        cache_misses: cacheStats.misses,
                        redis_hits: cacheStats.redis_hits,
                        api_calls: cacheStats.api_calls,
                        efficiency_percent: Math.round(cacheStats.hits / (cacheStats.hits + cacheStats.misses + 1) * 100)
                    }
                },
                enhancements: {
                    version: '2.1',
                    features: ['8_tier_priority_system', 'backwards_compatibility', 'aggressive_caching', 'smart_batch_sizing'],
                    processing_method: 'enhanced_attribution_with_fallbacks'
                },
                date_range: 'Past 7 days (safe range)'
            })
        };

    } catch (error) {
        console.error('❌ Enhanced attribution improvement system error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Enhanced attribution improvement system failed',
                details: error.message,
                version: '2.1'
            })
        };
    }
};

// Backwards compatible data extraction - handles missing enhanced fields gracefully
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
        
        // Processing hints
        requires_geo_correlation: !(conversion.session_id || conversion.device_signature),
        is_legacy_conversion: conversionAge > 30
    };
}

// Hash function for privacy-safe parameter values (matching other scripts)
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

// Enhanced cached geographic data strategy
async function getCachedGeoData(ip, geoDataCache, cacheStats) {
    if (!ip || ip === 'unknown') return null;
    
    // Check in-memory cache first (for current batch run)
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
    if (cacheStats.api_calls < 10) { // Limit API calls per batch run
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

// Enhanced attribution analysis with backwards compatibility
async function analyzeConversionForAttributionEnhanced(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('   🔬 Using backwards-compatible enhanced attribution system...');
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: conversion.landing_page || null,
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
            processing_path: 'unknown'
        }
    };
    
    // Try enhanced 8-tier system first (for conversions with enhanced params)
    if (conversionData.has_enhanced_params) {
        console.log('   🚀 Trying enhanced 8-tier attribution system...');
        results.analysis.processing_path = '8_tier_system';
        
        const enhancedResult = await findEnhancedAttribution(conversionData);
        
        if (enhancedResult) {
            results.matchFound = true;
            results.newAttribution = enhancedResult.landing_page || enhancedResult.url;
            results.match = enhancedResult;
            results.attributionMethod = enhancedResult.method;
            results.priorityLevel = getPriorityLevel(enhancedResult.method);
            
            const shouldUpdate = shouldUpdateAttributionByPriority(conversion, enhancedResult);
            results.shouldUpdate = shouldUpdate.update;
            results.improvementType = shouldUpdate.type;
            
            console.log(`   ✅ Enhanced attribution found: ${enhancedResult.method} (Priority ${results.priorityLevel})`);
            return results;
        }
    }
    
    // Fallback to geographic correlation (Priority 8) for legacy conversions or failed enhanced lookups
    console.log('   🌍 Falling back to geographic correlation...');
    results.analysis.processing_path = 'geographic_correlation';
    
    const geoResult = await performGeographicCorrelationCached(conversion, pageviews, conversionData, geoDataCache, cacheStats);
    
    if (geoResult) {
        results.matchFound = true;
        results.newAttribution = geoResult.newAttribution;
        results.match = geoResult.match;
        results.attributionMethod = geoResult.method || 'geo_correlation';
        results.priorityLevel = 8;
        
        const shouldUpdate = shouldUpdateAttributionByPriority(conversion, geoResult);
        results.shouldUpdate = shouldUpdate.update;
        results.improvementType = shouldUpdate.type;
        
        console.log(`   ✅ Geographic correlation successful: ${results.attributionMethod}`);
    } else {
        console.log('   ❌ No attribution found via any method');
        results.improvementType = 'NO_MATCH_FOUND';
    }
    
    return results;
}

// 8-tier enhanced attribution system (adapted for analytics data)
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

// Helper function to get priority level from method
function getPriorityLevel(method) {
    const priorities = {
        'ssid_direct_match': 1,
        'primary_ip_match': 2,
        'conversion_ip_match': 3,
        'pageview_ip_match': 4,
        'device_signature_match': 5,
        'screen_hash_match': 6,
        'webgl_signature_match': 7,
        'geo_correlation': 8,
        'geo_high_confidence': 8,
        'geo_medium_confidence': 8
    };
    return priorities[method] || 9;
}

// IPv6-safe key encoding
function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
}

// Enhanced geographic correlation with caching
async function performGeographicCorrelationCached(conversion, pageviews, conversionData, geoDataCache, cacheStats) {
    console.log('      🌍 Starting cached geographic correlation...');
    
    // Get the best IP for correlation
    const testIPs = [conversionData.PIP, conversionData.CIP, conversionData.IP].filter(Boolean);
    
    if (testIPs.length === 0) {
        console.log('      ❌ No IPs available for geographic correlation');
        return null;
    }
    
    // Find pageviews in 90-minute window before conversion
    const candidatePageviews = findPageviewsIn90MinuteWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log('      ❌ No pageviews found in 90-minute window before conversion');
        return null;
    }
    
    console.log(`      📱 Found ${candidatePageviews.length} pageviews in 90-minute window`);
    
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

        // Find the best temporal match with geographic correlation
        const bestMatch = await findBestTemporalMatchCached(conversion, candidatePageviews, conversionGeo, geoDataCache, cacheStats);
        
        if (bestMatch) {
            console.log(`      ✅ Geographic correlation successful: ${bestMatch.confidence} (score: ${Math.round(bestMatch.score)})`);
            
            return {
                newAttribution: bestMatch.pageview.landing_page || bestMatch.pageview.url,
                match: bestMatch,
                method: bestMatch.confidence === 'HIGH_CONFIDENCE' ? 'geo_high_confidence' : 
                       bestMatch.confidence === 'MEDIUM_CONFIDENCE' ? 'geo_medium_confidence' : 'geo_correlation'
            };
        }
    }
    
    console.log('      ❌ Geographic correlation failed for all IPs');
    return null;
}

// Find pageviews in 90-minute window before conversion
function findPageviewsIn90MinuteWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 90 * 60 * 1000); // 90 minutes before
    
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

// Find best temporal match with cached geographic data
async function findBestTemporalMatchCached(conversion, candidatePageviews, conversionGeoData, geoDataCache, cacheStats) {
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
        
        // Progress logging for large datasets
        if ((i + 1) % 25 === 0) {
            console.log(`      📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
        }
    }
    
    return null;
}

// Enhanced geographic scoring to match track.js system
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

// Compare ISPs with normalization
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    return normalizeISP(isp1) === normalizeISP(isp2);
}

// Normalize ISP names for better matching
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

// Priority-based update logic
function shouldUpdateAttributionByPriority(conversion, newMatch) {
    const hasCurrentAttribution = conversion.landing_page && conversion.landing_page !== '';
    
    if (!hasCurrentAttribution) {
        return { update: true, type: 'NEW_ATTRIBUTION' };
    }
    
    // Get current attribution method priority (if available)
    const currentMethod = conversion.attribution_improvement?.method || 'unknown';
    const currentPriority = getPriorityLevel(currentMethod);
    const newPriority = getPriorityLevel(newMatch.method);
    
    // Update if new method has higher priority (lower number = higher priority)
    if (newPriority < currentPriority) {
        return { update: true, type: 'PRIORITY_UPGRADE' };
    }
    
    // For same priority level, use temporal precedence
    if (newPriority === currentPriority) {
        const currentAttributionTime = conversion.attributed_pageview_timestamp || conversion.timestamp;
        const newMatchTime = newMatch.pageview?.timestamp || newMatch.timestamp;
        
        if (newMatchTime && new Date(newMatchTime) < new Date(currentAttributionTime)) {
            return { update: true, type: 'TEMPORAL_IMPROVEMENT' };
        }
    }
    
    // Keep existing if lower or same priority
    return { update: false, type: 'NO_IMPROVEMENT' };
}

// Enhanced attribution update with version tracking
async function updateConversionAttributionEnhanced(conversion, improvementResults) {
    try {
        // Find the conversion record in Redis
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            // Get existing data
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            // Update with enhanced attribution
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match.pageview?.source || improvementResults.match.source || 'enhanced_v2_1',
                utm_campaign: improvementResults.match.pageview?.utm_campaign || improvementResults.match.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match.pageview?.utm_medium || improvementResults.match.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match.pageview?.referrer_url || improvementResults.match.referrer_url || conversionData.referrer_url,
                
                // Enhanced attribution improvement metadata
                attribution_improvement: {
                    method: improvementResults.attributionMethod,
                    improvement_type: improvementResults.improvementType,
                    priority_level: improvementResults.priorityLevel,
                    confidence: improvementResults.match.confidence || 'medium',
                    score: improvementResults.match.score || 0,
                    time_difference_minutes: improvementResults.match.timeDiff || 0,
                    improved_at: new Date().toISOString(),
                    system_version: '2.1',
                    processing_path: improvementResults.analysis?.processing_path || 'unknown',
                    
                    // Attribution source metadata
                    pageview_ip: improvementResults.match.pageview?.ip_address || improvementResults.match.ip_address,
                    pageview_timestamp: improvementResults.match.pageview?.timestamp || improvementResults.match.timestamp,
                    matched_ip_type: improvementResults.match.matched_ip || 'unknown'
                },
                
                // Store previous attribution if it existed
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

// Enhanced Process3 marking with version tracking
async function markConversionAsProcess3Enhanced(conversion, attributionMethod) {
    try {
        const process3Key = `process3:${conversion.email}:${conversion.timestamp}`;
        const process3Data = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'attribution_improvement_8tier_backwards_compatible',
            version: '2.1',
            attribution_method: attributionMethod,
            processing_type: 'batch_reprocessing_enhanced'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', process3Key, 2592000, JSON.stringify(process3Data)); // 30 days
    } catch (error) {
        console.log(`   ⚠️ Could not mark ${conversion.email} as process3: ${error.message}`);
    }
}

// EXISTING FUNCTIONS (maintained for compatibility)

// Fetch analytics data for past 7 days (same as original)
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

// Get ALL conversions (same as original)
function getAllConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    const sortedConversions = conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`📊 Found ${sortedConversions.length} total conversions for enhanced reprocessing evaluation`);
    
    return sortedConversions;
}

// Filter out conversions already marked as "process3"
async function filterNonProcess3Conversions(allConversions) {
    const nonProcess3Conversions = [];
    let process3Count = 0;
    
    for (const conversion of allConversions) {
        const process3Key = `process3:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const process3Data = await redisRequest('get', process3Key);
            
            if (process3Data) {
                process3Count++;
            } else {
                nonProcess3Conversions.push(conversion);
            }
        } catch (error) {
            // If we can't check status, assume not processed
            nonProcess3Conversions.push(conversion);
        }
    }
    
    console.log(`📊 Process3 status: ${process3Count} already processed, ${nonProcess3Conversions.length} remaining`);
    
    return nonProcess3Conversions;
}

// Redis request helper (same as original)
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

// Find conversion key in Redis (same as original)
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
        console.error(`❌ Error finding conversion key for ${conversion.email}:`, error);
        return null;
    }
}
