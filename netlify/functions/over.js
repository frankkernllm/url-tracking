exports.handler = async (event, context) => {
    // POSSIBLE ATTRIBUTION REPROCESSING SYSTEM: Stricter City-Match Criteria
    // Re-evaluates conversions with "POSSIBLE" attribution confidence using stricter criteria
    // Requires city matches to maintain attribution, otherwise removes attribution
    
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
        console.log('🔧 Starting POSSIBLE ATTRIBUTION REPROCESSING SYSTEM v1.0');
        console.log('🎯 Target: Conversions with POSSIBLE attribution confidence');
        console.log('📏 Criteria: City-match required to maintain attribution');
        
        // Timeout protection
        const startTime = Date.now();
        const maxRunTime = 20000; // 20 seconds
        let processedInThisRun = 0;
        const processedConversions = [];
        
        // Step 1: Fetch analytics data from past 7 days
        const analyticsData = await fetchAnalyticsDataPast7Days();
        
        // Step 2: Find all conversions
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
        
        console.log(`🔍 REPROCESSING TARGET: POSSIBLE attribution conversions only`);
        
        // Step 3: Get conversions with POSSIBLE attribution confidence (not already reprocessed)
        console.log('🔍 Finding conversions with POSSIBLE attribution confidence...');
        let possibleConversions = await filterPossibleAttributionConversions(allConversions);
        
        if (possibleConversions.length === 0) {
            console.log('🎉 NO POSSIBLE CONVERSIONS FOUND TO REPROCESS!');
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No POSSIBLE attribution conversions found to reprocess',
                    progress: { 
                        total_conversions: allConversions.length, 
                        possible_conversions: 0, 
                        remaining_conversions: 0, 
                        status: 'NO_POSSIBLE_FOUND' 
                    }
                })
            };
        }
        
        console.log(`📋 Found ${possibleConversions.length} POSSIBLE attribution conversions to reprocess`);
        console.log(`📊 Estimated batches needed: ${Math.ceil(possibleConversions.length / 3)}`);
        
        // Main reprocessing loop - process conversions in batches of 3
        while (possibleConversions.length > 0 && Date.now() - startTime < maxRunTime) {
            
            // Check if we have enough time for another batch
            const timeRemaining = maxRunTime - (Date.now() - startTime);
            if (timeRemaining < 4000) {
                console.log(`⏰ Approaching timeout: ${timeRemaining/1000}s remaining - stopping batch processing`);
                break;
            }
            
            // Step 4: Get next batch of 3 conversions
            const batchSize = Math.min(3, possibleConversions.length);
            const currentBatch = possibleConversions.splice(0, batchSize);
            const batchStartTime = Date.now();
            
            console.log(`\n📦 REPROCESSING BATCH: ${batchSize} conversions (${possibleConversions.length} remaining after this batch)`);
            
            // Process each conversion in the current batch
            for (let i = 0; i < currentBatch.length; i++) {
                const conversionToProcess = currentBatch[i];
                const conversionStartTime = Date.now();
                
                console.log(`\n🎯 REPROCESSING ${i + 1}/${batchSize}: ${conversionToProcess.email}`);
                console.log(`   📍 IP: ${conversionToProcess.ip_address}`);
                console.log(`   ⏰ Time: ${conversionToProcess.timestamp}`);
                console.log(`   📊 Current POSSIBLE Attribution: ${conversionToProcess.landing_page || 'NONE'}`);
                
                // Step 5: Re-analyze with stricter criteria
                const reprocessResults = await reanalyzeConversionWithStricterCriteria(conversionToProcess, analyticsData.page_views);
                
                // Step 6: Update Redis based on stricter evaluation
                let updateResult = null;
                if (reprocessResults.shouldUpdate) {
                    console.log(`📝 Updating attribution for ${conversionToProcess.email}...`);
                    try {
                        updateResult = await updateConversionAttributionStrict(conversionToProcess, reprocessResults);
                    } catch (redisError) {
                        console.error('❌ Redis update failed:', redisError);
                    }
                }
                
                // Step 7: Mark as reprocessed
                await markConversionAsReprocessed(conversionToProcess);
                
                // Track this processed conversion
                const conversionTime = Date.now() - conversionStartTime;
                processedInThisRun++;
                processedConversions.push({
                    email: conversionToProcess.email,
                    reprocess_result: reprocessResults.resultType,
                    processing_time_ms: conversionTime,
                    update_result: updateResult
                });
                
                console.log(`✅ Reprocessed ${conversionToProcess.email} in ${conversionTime/1000}s - ${reprocessResults.resultType}`);
            }
            
            // Batch completion summary
            const batchTime = Date.now() - batchStartTime;
            console.log(`📦 Batch complete: ${batchSize} conversions in ${batchTime/1000}s | Total reprocessed: ${processedInThisRun}`);
        }
        
        // Final status calculation
        const totalTime = Date.now() - startTime;
        const totalRemaining = possibleConversions.length;
        const isComplete = totalRemaining === 0;
        
        console.log(`\n🏁 REPROCESSING COMPLETE:`);
        console.log(`   ⏱️  Total run time: ${totalTime/1000}s`);
        console.log(`   ✅ Reprocessed in this run: ${processedInThisRun}`);
        console.log(`   🔄 Remaining POSSIBLE conversions: ${totalRemaining}`);
        console.log(`   🎯 Status: ${isComplete ? 'ALL POSSIBLE CONVERSIONS REPROCESSED' : 'RUN AGAIN TO CONTINUE'}`);
        
        // Generate summary message
        let summaryMessage;
        if (isComplete) {
            summaryMessage = `🎉 ALL POSSIBLE CONVERSIONS REPROCESSED! Evaluated ${processedInThisRun} conversions with stricter criteria.`;
        } else if (processedInThisRun > 0) {
            summaryMessage = `✅ Reprocessed ${processedInThisRun} POSSIBLE conversions (${totalRemaining} remaining). Run again to continue.`;
        } else {
            summaryMessage = `⚠️ No conversions reprocessed (may be near timeout limit). ${totalRemaining} POSSIBLE conversions remaining.`;
        }
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                reprocessing_complete: true,
                processed_this_run: processedInThisRun,
                processed_conversions: processedConversions,
                message: summaryMessage,
                progress: {
                    total_conversions: allConversions.length,
                    possible_conversions_found: processedInThisRun + totalRemaining,
                    reprocessed_this_batch: processedInThisRun,
                    remaining_possible_conversions: totalRemaining,
                    status: isComplete ? 'ALL_POSSIBLE_COMPLETE' : 'CONTINUE_REPROCESSING',
                    next_action: isComplete ? 'All POSSIBLE conversions reprocessed with stricter criteria!' : 'Run the function again to continue reprocessing'
                },
                performance: {
                    total_run_time_seconds: totalTime / 1000,
                    average_time_per_conversion: processedInThisRun > 0 ? (totalTime / processedInThisRun / 1000) : 0,
                    timeout_limit_seconds: maxRunTime / 1000
                },
                date_range: 'Past 7 days (safe range)',
                processing_method: 'strict_reprocessing_batch',
                criteria: 'City-match required to maintain attribution'
            })
        };

    } catch (error) {
        console.error('❌ POSSIBLE attribution reprocessing system error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'POSSIBLE attribution reprocessing system failed',
                details: error.message
            })
        };
    }
};

// Global counters for cache statistics
let cacheStats = {
    hits: 0,
    misses: 0,
    errors: 0
};

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
    
    console.log(`📊 Found ${sortedConversions.length} total conversions for reprocessing evaluation`);
    
    return sortedConversions;
}

// NEW: Filter conversions with POSSIBLE attribution confidence (not already reprocessed)
async function filterPossibleAttributionConversions(allConversions) {
    const possibleConversions = [];
    let alreadyReprocessedCount = 0;
    let possibleCount = 0;
    
    for (const conversion of allConversions) {
        // Check if already reprocessed
        const reprocessedKey = `reprocessed_strict:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const reprocessedData = await redisRequest('get', reprocessedKey);
            
            if (reprocessedData) {
                alreadyReprocessedCount++;
                continue;
            }
            
            // Check if this conversion has POSSIBLE attribution confidence
            const hasPossibleAttribution = await checkForPossibleAttribution(conversion);
            
            if (hasPossibleAttribution) {
                possibleCount++;
                possibleConversions.push(conversion);
                console.log(`   🔍 POSSIBLE: ${conversion.email} - ${conversion.landing_page || 'NO_ATTRIBUTION'}`);
            }
            
        } catch (error) {
            // If we can't check status, skip for safety
            console.log(`   ⚠️ Could not check reprocessing status for ${conversion.email}`);
        }
    }
    
    console.log(`📊 Attribution confidence breakdown:`);
    console.log(`   🔍 POSSIBLE attributions found: ${possibleCount}`);
    console.log(`   ✅ Already reprocessed: ${alreadyReprocessedCount}`);
    console.log(`   🎯 Available for reprocessing: ${possibleConversions.length}`);
    
    return possibleConversions;
}

// Check if conversion has POSSIBLE attribution confidence
async function checkForPossibleAttribution(conversion) {
    try {
        // Find the conversion record in Redis to check attribution_improvement.confidence
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            // Check if it has attribution_improvement.confidence === 'POSSIBLE'
            if (conversionData.attribution_improvement && 
                conversionData.attribution_improvement.confidence === 'POSSIBLE') {
                return true;
            }
        }
        
        return false;
        
    } catch (error) {
        return false;
    }
}

// NEW: Re-analyze conversion with stricter criteria (city-match required)
async function reanalyzeConversionWithStricterCriteria(conversion, pageviews) {
    console.log('   🔬 Re-analyzing with STRICTER criteria (city-match required)...');
    
    cacheStats = { hits: 0, misses: 0, errors: 0 };
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: conversion.landing_page || null,
        strictMatchFound: false,
        newAttribution: null,
        shouldUpdate: false,
        resultType: 'NO_CHANGE',
        match: null,
        analysis: {
            pageviews_in_window: 0,
            cache_performance: {}
        }
    };
    
    // Find all pageviews in 90-minute window before conversion
    const candidatePageviews = findPageviewsIn90MinuteWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log(`   ❌ No pageviews found in 90-minute window - REMOVE attribution`);
        results.shouldUpdate = true;
        results.resultType = 'ATTRIBUTION_REMOVED';
        results.newAttribution = null;
        return results;
    }
    
    console.log(`   📱 Found ${candidatePageviews.length} pageviews in 90-minute window`);
    results.analysis.pageviews_in_window = candidatePageviews.length;
    
    // Get conversion geographic data
    const conversionGeoData = await getIPLocationData(conversion.ip_address);
    console.log(`   📍 Location: ${conversionGeoData.city}, ${conversionGeoData.region}`);
    
    // Find the best match with STRICT criteria (city match required)
    const bestStrictMatch = await findBestStrictMatch(conversion, candidatePageviews, conversionGeoData);
    
    if (bestStrictMatch) {
        console.log(`   ✅ STRICT MATCH FOUND! (${bestStrictMatch.confidence}) - KEEP/UPGRADE attribution`);
        
        results.strictMatchFound = true;
        results.newAttribution = bestStrictMatch.pageview.landing_page || bestStrictMatch.pageview.url;
        results.match = bestStrictMatch;
        results.shouldUpdate = true;
        
        // Determine if we're upgrading or keeping
        if (bestStrictMatch.confidence === 'DEFINITE' || bestStrictMatch.confidence === 'STRONG') {
            results.resultType = 'ATTRIBUTION_UPGRADED';
        } else {
            results.resultType = 'ATTRIBUTION_KEPT';
        }
        
    } else {
        console.log('   ❌ No STRICT matches found (no city match) - REMOVE attribution');
        results.shouldUpdate = true;
        results.resultType = 'ATTRIBUTION_REMOVED';
        results.newAttribution = null;
    }
    
    // Cache performance stats
    const totalLookups = cacheStats.hits + cacheStats.misses + cacheStats.errors;
    const hitRate = totalLookups > 0 ? ((cacheStats.hits / totalLookups) * 100).toFixed(1) : 0;
    
    results.analysis.cache_performance = {
        cache_hits: cacheStats.hits,
        cache_misses: cacheStats.misses,
        cache_hit_rate_percent: hitRate
    };
    
    return results;
}

// Find pageviews in 90-minute window (same as original)
function findPageviewsIn90MinuteWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 90 * 60 * 1000);
    
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address;
    });
    
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return candidatePageviews;
}

// NEW: Find best match with STRICT criteria (city match required)
async function findBestStrictMatch(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        // Get pageview geographic data
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        
        // Use STRICT geographic comparison (city match required)
        const geoMatch = compareGeographicDataStrict(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`   🏆 STRICT MATCH: ${timeDiff.toFixed(1)}min before (${geoMatch.confidence}) - City match required and found!`);
            
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
            console.log(`   📊 Checked ${i + 1}/${candidatePageviews.length} pageviews with strict criteria`);
        }
    }
    
    return null;
}

// NEW: STRICT geographic comparison - city match REQUIRED
function compareGeographicDataStrict(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    // STRICT CRITERIA: City match is REQUIRED
    if (!cityMatch) {
        return {
            isMatch: false,
            confidence: 'NO_CITY_MATCH',
            score: 0,
            cityMatch: false,
            regionMatch,
            countryMatch,
            ispMatch
        };
    }

    // If city matches, calculate score normally
    let score = 3; // Start with 3 for city match
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

    let confidence = 'POSSIBLE';
    
    if (score >= 6) { // City + region + country + ISP
        confidence = 'DEFINITE';
    } else if (score >= 5) { // City + region + country OR city + ISP + country
        confidence = 'STRONG';
    } else if (score >= 3) { // City match (minimum)
        confidence = 'POSSIBLE';
    }

    return {
        isMatch: true, // Only true if city matches
        confidence,
        score,
        cityMatch: true, // Always true if we reach here
        regionMatch,
        countryMatch,
        ispMatch
    };
}

// Mark conversion as reprocessed with strict criteria
async function markConversionAsReprocessed(conversion) {
    try {
        const reprocessedKey = `reprocessed_strict:${conversion.email}:${conversion.timestamp}`;
        const reprocessedData = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            reprocessed_at: new Date().toISOString(),
            system: 'strict_reprocessing_batch',
            criteria: 'city_match_required'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', reprocessedKey, 2592000, JSON.stringify(reprocessedData));
    } catch (error) {
        console.log(`   ⚠️ Could not mark ${conversion.email} as reprocessed: ${error.message}`);
    }
}

// NEW: Update conversion attribution with strict criteria (can remove attribution)
async function updateConversionAttributionStrict(conversion, reprocessResults) {
    try {
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            let updatedConversion;
            
            if (reprocessResults.resultType === 'ATTRIBUTION_REMOVED') {
                // Remove attribution - make it unattributed again
                updatedConversion = {
                    ...conversionData,
                    attribution_found: false,
                    landing_page: null,
                    source: null,
                    utm_campaign: null,
                    utm_medium: null,
                    referrer_url: null,
                    // Strict reprocessing metadata
                    attribution_improvement: {
                        ...conversionData.attribution_improvement,
                        strict_reprocessing: {
                            method: 'strict_city_match_required',
                            result: 'attribution_removed',
                            reason: 'no_city_match_found',
                            reprocessed_at: new Date().toISOString(),
                            previous_confidence: conversionData.attribution_improvement?.confidence || 'UNKNOWN'
                        }
                    },
                    // Store what was removed
                    removed_attribution: {
                        landing_page: conversionData.landing_page,
                        confidence: conversionData.attribution_improvement?.confidence,
                        removed_at: new Date().toISOString()
                    },
                    attributed_pageview_timestamp: null
                };
                
            } else {
                // Keep or upgrade attribution
                updatedConversion = {
                    ...conversionData,
                    attribution_found: true,
                    landing_page: reprocessResults.newAttribution,
                    source: reprocessResults.match.pageview.source || 'strict_reprocessed',
                    utm_campaign: reprocessResults.match.pageview.utm_campaign || conversionData.utm_campaign,
                    utm_medium: reprocessResults.match.pageview.utm_medium || conversionData.utm_medium,
                    referrer_url: reprocessResults.match.pageview.referrer_url || conversionData.referrer_url,
                    // Enhanced attribution improvement metadata
                    attribution_improvement: {
                        ...conversionData.attribution_improvement,
                        confidence: reprocessResults.match.confidence, // Potentially upgraded
                        strict_reprocessing: {
                            method: 'strict_city_match_required',
                            result: reprocessResults.resultType.toLowerCase(),
                            previous_confidence: conversionData.attribution_improvement?.confidence || 'UNKNOWN',
                            new_confidence: reprocessResults.match.confidence,
                            score: reprocessResults.match.score,
                            time_difference_minutes: reprocessResults.match.timeDiff,
                            reprocessed_at: new Date().toISOString(),
                            pageview_ip: reprocessResults.match.pageview.ip_address,
                            pageview_timestamp: reprocessResults.match.pageview.timestamp
                        }
                    },
                    attributed_pageview_timestamp: reprocessResults.match.pageview.timestamp
                };
            }
            
            // Save back to Redis
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                result_type: reprocessResults.resultType,
                new_attribution: reprocessResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
                confidence_change: conversionData.attribution_improvement?.confidence + ' → ' + (reprocessResults.match?.confidence || 'REMOVED')
            };
            
        } else {
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        return { success: false, error: error.message };
    }
}

// All remaining functions are the same as the original...
// (getIPLocationData, extractBestISP, compareISPs, redisRequest, findConversionKey, etc.)

async function getCachedGeoData(ip) {
    try {
        const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
        const cachedResult = await redisRequest('get', cacheKey);
        
        if (cachedResult === null || cachedResult === undefined) {
            cacheStats.misses++;
            return null;
        }
        
        if (cachedResult) {
            try {
                const cachedData = JSON.parse(decodeURIComponent(cachedResult));
                cacheStats.hits++;
                return cachedData;
            } catch (parseError) {
                cacheStats.errors++;
                return null;
            }
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        cacheStats.errors++;
        return null;
    }
}

async function getIPLocationData(ip) {
    const cached = await getCachedGeoData(ip);
    if (cached) return cached;
    
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' },
            signal: AbortSignal.timeout(2000)
        });
        
        if (response.ok) {
            const data = await response.json();
            
            const geoData = {
                ip: data.ip,
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                coordinates: data.loc || '0,0',
                timezone: data.timezone || 'Unknown',
                lookup_timestamp: new Date().toISOString()
            };
            
            try {
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                const encodedData = encodeURIComponent(JSON.stringify(geoData));
                await redisRequest('setex', cacheKey, 86400, encodedData);
            } catch (cacheError) {
                // Ignore cache errors
            }
            
            return geoData;
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        return {
            ip: ip,
            city: 'LOOKUP_FAILED',
            region: 'LOOKUP_FAILED',
            country: 'LOOKUP_FAILED',
            isp: 'LOOKUP_FAILED'
        };
    }
}

function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = (str) => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    if (norm1 === norm2) return true;
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

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
