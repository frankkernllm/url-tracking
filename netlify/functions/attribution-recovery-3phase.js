exports.handler = async (event, context) => {
    // ATTRIBUTION IMPROVEMENT SYSTEM: Stricter Geographic Criteria
    // Processes multiple conversions in one run with timeout protection
    // Uses stricter geographic matching - requires score >= 4 instead of >= 3
    
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
        console.log('🔧 Starting ATTRIBUTION IMPROVEMENT SYSTEM - STRICTER CRITERIA v1.0');
        console.log('⚡ Will reprocess multiple conversions with stricter geographic matching');
        
        // Timeout protection - more aggressive timing
        const startTime = Date.now();
        const maxRunTime = 20000; // 20 seconds (tighter buffer)
        let processedInThisRun = 0;
        const processedConversions = [];
        
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
        
        console.log(`🔄 BATCH PROCESSING: Will process in batches of 3 for max ${maxRunTime/1000} seconds`);
        
        // Step 3: Get initial list of non-process3 conversions (ONCE at start)
        console.log('🔍 Getting initial list of non-process3 conversions...');
        let unprocessedConversions = await filterNonProcess3Conversions(allConversions);
        
        if (unprocessedConversions.length === 0) {
            console.log('🎉 ALL CONVERSIONS ALREADY COMPLETED WITH STRICTER CRITERIA!');
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'All conversions already completed with stricter criteria',
                    progress: { total_conversions: allConversions.length, total_processed: allConversions.length, remaining_conversions: 0, status: 'ALL_COMPLETE' }
                })
            };
        }
        
        console.log(`📋 Found ${unprocessedConversions.length} non-process3 conversions (will process in batches of 3)`);
        console.log(`📊 Estimated batches needed: ${Math.ceil(unprocessedConversions.length / 3)}`);
        
        // Main processing loop - process conversions in batches of 3
        while (unprocessedConversions.length > 0 && Date.now() - startTime < maxRunTime) {
            
            // Check if we have enough time for another batch
            const timeRemaining = maxRunTime - (Date.now() - startTime);
            if (timeRemaining < 4000) {
                console.log(`⏰ Approaching timeout: ${timeRemaining/1000}s remaining - stopping batch processing`);
                break;
            }
            
            // Step 4: Get next batch of 3 conversions (or remaining if less than 3)
            const batchSize = Math.min(3, unprocessedConversions.length);
            const currentBatch = unprocessedConversions.splice(0, batchSize); // Remove from front of array
            const batchStartTime = Date.now();
            
            console.log(`\n📦 PROCESSING BATCH: ${batchSize} conversions (${unprocessedConversions.length} remaining after this batch)`);
            
            // Process each conversion in the current batch
            for (let i = 0; i < currentBatch.length; i++) {
                const conversionToProcess = currentBatch[i];
                const conversionStartTime = Date.now();
                
                console.log(`\n🎯 CONVERSION ${i + 1}/${batchSize}: ${conversionToProcess.email}`);
                console.log(`   📍 IP: ${conversionToProcess.ip_address}`);
                console.log(`   ⏰ Time: ${conversionToProcess.timestamp}`);
                console.log(`   📊 Current: ${conversionToProcess.landing_page || 'NONE'}`);
                
                // Step 5: Analyze this conversion with stricter criteria
                const improvementResults = await analyzeConversionForAttribution(conversionToProcess, analyticsData.page_views);
                
                // Step 6: Update Redis if needed
                let updateResult = null;
                if (improvementResults.shouldUpdate) {
                    console.log(`📝 Updating attribution for ${conversionToProcess.email}...`);
                    try {
                        updateResult = await updateConversionAttribution(conversionToProcess, improvementResults);
                    } catch (redisError) {
                        console.error('❌ Redis update failed:', redisError);
                    }
                }
                
                // Step 7: Mark as process3
                await markConversionAsProcess3(conversionToProcess);
                
                // Track this processed conversion
                const conversionTime = Date.now() - conversionStartTime;
                processedInThisRun++;
                processedConversions.push({
                    email: conversionToProcess.email,
                    improvement_type: improvementResults.improvementType,
                    processing_time_ms: conversionTime,
                    update_result: updateResult
                });
                
                console.log(`✅ Completed ${conversionToProcess.email} in ${conversionTime/1000}s`);
            }
            
            // Batch completion summary
            const batchTime = Date.now() - batchStartTime;
            console.log(`📦 Batch complete: ${batchSize} conversions in ${batchTime/1000}s | Total reprocessed: ${processedInThisRun}`);
        }
        
        // Final status calculation
        const totalTime = Date.now() - startTime;
        const totalRemaining = unprocessedConversions.length; // Remaining in our list
        const totalProcessed = allConversions.length - totalRemaining;
        const isComplete = totalRemaining === 0;
        
        console.log(`\n🏁 RUN COMPLETE:`);
        console.log(`   ⏱️  Total run time: ${totalTime/1000}s`);
        console.log(`   ✅ Processed in this run: ${processedInThisRun}`);
        console.log(`   📊 Total processed overall: ${totalProcessed}/${allConversions.length}`);
        console.log(`   🔄 Remaining: ${totalRemaining}`);
        console.log(`   🎯 Status: ${isComplete ? 'ALL COMPLETE' : 'RUN AGAIN TO CONTINUE'}`);
        
        // Generate summary message
        let summaryMessage;
        if (isComplete) {
            summaryMessage = `🎉 ALL ${allConversions.length} CONVERSIONS COMPLETED! Processed ${processedInThisRun} conversions in final run.`;
        } else if (processedInThisRun > 0) {
            summaryMessage = `✅ Processed ${processedInThisRun} conversions (${totalRemaining} remaining). Run again to continue optimizing.`;
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
                    next_action: isComplete ? 'All conversions optimized!' : 'Run the function again to continue processing'
                },
                performance: {
                    total_run_time_seconds: totalTime / 1000,
                    average_time_per_conversion: processedInThisRun > 0 ? (totalTime / processedInThisRun / 1000) : 0,
                    timeout_limit_seconds: maxRunTime / 1000
                },
                date_range: 'Past 7 days (safe range)',
                processing_method: 'stricter_geographic_criteria'
            })
        };

    } catch (error) {
        console.error('❌ Attribution improvement system error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Attribution improvement system failed',
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
    
    return nonProcess3Conversions;
}

// Analyze single conversion for attribution improvement
async function analyzeConversionForAttribution(conversion, pageviews) {
    console.log('   🔬 Finding best temporal match...');
    
    // Reset cache statistics
    cacheStats = { hits: 0, misses: 0, errors: 0 };
    
    const results = {
        conversionEmail: conversion.email,
        originalAttribution: conversion.landing_page || null,
        matchFound: false,
        newAttribution: null,
        shouldUpdate: false,
        improvementType: 'NO_CHANGE',
        match: null,
        analysis: {
            pageviews_in_window: 0,
            cache_performance: {}
        }
    };
    
    // Find all pageviews in 90-minute window before conversion
    const candidatePageviews = findPageviewsIn90MinuteWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log(`   ❌ No pageviews found in 90-minute window before conversion`);
        results.analysis.pageviews_in_window = 0;
        return results;
    }
    
    console.log(`   📱 Found ${candidatePageviews.length} pageviews in 90-minute window`);
    results.analysis.pageviews_in_window = candidatePageviews.length;
    
    // Get conversion geographic data
    const conversionGeoData = await getIPLocationData(conversion.ip_address);
    console.log(`   📍 Location: ${conversionGeoData.city}, ${conversionGeoData.region} (${conversionGeoData.isp})`);
    
    // Find the best temporal match
    const bestMatch = await findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData);
    
    if (bestMatch) {
        console.log(`   ✅ MATCH FOUND! (${bestMatch.confidence})`);
        
        results.matchFound = true;
        results.newAttribution = bestMatch.pageview.landing_page || bestMatch.pageview.url;
        results.match = bestMatch;
        
        // Determine if we should update
        const shouldUpdate = shouldUpdateAttribution(conversion, bestMatch);
        results.shouldUpdate = shouldUpdate.update;
        results.improvementType = shouldUpdate.type;
        
        console.log(`   🎯 Update: ${shouldUpdate.update ? 'YES' : 'NO'} (${shouldUpdate.type})`);
        
    } else {
        console.log('   ❌ No geographic matches found');
        results.improvementType = 'NO_MATCH_FOUND';
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

// Find best temporal match (closest to conversion time)
async function findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        const ipType = pageview.ip_address.includes(':') ? 'IPv6' : 'IPv4';
        
        // Get pageview geographic data
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        
        // Compare geographic data
        const geoMatch = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`   🏆 TEMPORAL MATCH: ${timeDiff.toFixed(1)}min before (${geoMatch.confidence})`);
            
            return {
                pageview: pageview,
                score: geoMatch.score,
                timeDiff: timeDiff,
                confidence: geoMatch.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData,
                ipType: ipType,
                candidateNumber: i + 1
            };
        }
        
        // Progress logging for large datasets
        if ((i + 1) % 25 === 0) {
            console.log(`   📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
        }
    }
    
    return null;
}

// Determine if attribution should be updated
function shouldUpdateAttribution(conversion, newMatch) {
    const hasCurrentAttribution = conversion.landing_page && conversion.landing_page !== '';
    
    if (!hasCurrentAttribution) {
        return { update: true, type: 'NEW_ATTRIBUTION' };
    }
    
    // Has existing attribution - check temporal precedence
    const currentAttributionTime = conversion.attributed_pageview_timestamp || conversion.timestamp;
    const newMatchTime = newMatch.pageview.timestamp;
    
    if (new Date(newMatchTime) < new Date(currentAttributionTime)) {
        return { update: true, type: 'TEMPORAL_IMPROVEMENT' };
    } else {
        return { update: false, type: 'NO_IMPROVEMENT' };
    }
}

// Update conversion attribution in Redis
async function updateConversionAttribution(conversion, improvementResults) {
    try {
        // Find the conversion record in Redis
        const conversionKey = await findConversionKey(conversion);
        
        if (conversionKey) {
            // Get existing data
            const existingData = await redisRequest('get', conversionKey);
            let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
            
            // Update with improved attribution
            const updatedConversion = {
                ...conversionData,
                attribution_found: true,
                landing_page: improvementResults.newAttribution,
                source: improvementResults.match.pageview.source || 'improved',
                utm_campaign: improvementResults.match.pageview.utm_campaign || conversionData.utm_campaign,
                utm_medium: improvementResults.match.pageview.utm_medium || conversionData.utm_medium,
                referrer_url: improvementResults.match.pageview.referrer_url || conversionData.referrer_url,
                // Attribution improvement metadata
                attribution_improvement: {
                    method: 'stricter_geographic_criteria',
                    improvement_type: improvementResults.improvementType,
                    confidence: improvementResults.match.confidence,
                    score: improvementResults.match.score,
                    time_difference_minutes: improvementResults.match.timeDiff,
                    improved_at: new Date().toISOString(),
                    pageview_ip: improvementResults.match.pageview.ip_address,
                    pageview_timestamp: improvementResults.match.pageview.timestamp
                },
                // Store previous attribution if it existed
                previous_attribution: conversionData.landing_page || null,
                attributed_pageview_timestamp: improvementResults.match.pageview.timestamp
            };
            
            // Save back to Redis
            await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
                improvement_type: improvementResults.improvementType
            };
            
        } else {
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        return { success: false, error: error.message };
    }
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

// Compare geographic data - STRICTER CRITERIA
function compareGeographicData(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    if (cityMatch) score += 3;
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

    let confidence = 'NO_MATCH';
    let isMatch = false;

    if (score >= 5) {
        confidence = 'DEFINITE';
        isMatch = true;
    } else if (score >= 4) {
        confidence = 'STRONG';
        isMatch = true;
    } else if (score >= 3) {
        confidence = 'POSSIBLE';
        isMatch = false; // ← STRICTER: Reject city-only matches
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

// Mark conversion as process3
async function markConversionAsProcess3(conversion) {
    try {
        const process3Key = `process3:${conversion.email}:${conversion.timestamp}`;
        const process3Data = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'attribution_improvement_stricter_criteria'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', process3Key, 2592000, JSON.stringify(process3Data)); // 30 days
    } catch (error) {
        console.log(`   ⚠️ Could not mark ${conversion.email} as process3: ${error.message}`);
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
