exports.handler = async (event, context) => {
    // ATTRIBUTION IMPROVEMENT SYSTEM: Single Conversion Processing
    // Processes one conversion at a time with comprehensive 24-hour attribution analysis
    // Improves existing attribution by finding temporally closer matches
    
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
        console.log('🔧 Starting ATTRIBUTION IMPROVEMENT SYSTEM - VERSION FIXED');
        console.log('📋 Processing single conversion with comprehensive 24-hour attribution analysis');
        
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
        
        // Step 3: Filter out "newly updated" conversions
        console.log('🔍 Filtering out already processed conversions...');
        const unprocessedConversions = await filterNewlyUpdatedConversions(allConversions);
        
        if (unprocessedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'All conversions from past 7 days have been processed!',
                    results: { 
                        total: allConversions.length, 
                        processed: allConversions.length, 
                        remaining: 0 
                    },
                    status: 'COMPLETE',
                    next_action: 'All recent conversions have been optimized'
                })
            };
        }
        
        // Step 4: Take the first unprocessed conversion
        const conversionToProcess = unprocessedConversions[0];
        
        console.log(`🎯 PROCESSING CONVERSION: ${conversionToProcess.email}`);
        console.log(`   📍 Conversion IP: ${conversionToProcess.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${conversionToProcess.timestamp}`);
        console.log(`   📊 Current Attribution: ${conversionToProcess.landing_page || 'NONE'}`);
        console.log(`   📈 Progress: Processing 1 of ${unprocessedConversions.length} remaining conversions`);
        
        // Step 5: Analyze this conversion with 24-hour attribution improvement
        const improvementResults = await analyzeConversionForAttribution(conversionToProcess, analyticsData.page_views);
        
        // Step 6: Update Redis with improved attribution
        let updateResult = null;
        if (improvementResults.shouldUpdate) {
            console.log(`📝 Updating attribution for ${conversionToProcess.email}...`);
            try {
                updateResult = await updateConversionAttribution(conversionToProcess, improvementResults);
            } catch (redisError) {
                console.error('❌ Redis update failed:', redisError);
            }
        }
        
        // Step 7: Mark conversion as "newly updated"
        await markConversionAsNewlyUpdated(conversionToProcess);
        
        // Step 8: Calculate completion status more accurately - FIXED VARIABLES
        const totalProcessedAfterThis = allConversions.length - unprocessedConversions.length + 1;
        const totalRemainingAfterThis = allConversions.length - totalProcessedAfterThis;
        const isComplete = totalRemainingAfterThis === 0;
        
        console.log(`📊 PROGRESS UPDATE:`);
        console.log(`   📈 Total conversions: ${allConversions.length}`);
        console.log(`   ✅ Processed (including current): ${totalProcessedAfterThis}`);
        console.log(`   🔄 Remaining to process: ${totalRemainingAfterThis}`);
        console.log(`   🏁 Status: ${isComplete ? 'COMPLETE' : 'CONTINUE'}`);
        
        // Step 9: Return processing status - FIXED VARIABLES
        const statusMessage = generateStatusMessage(conversionToProcess, improvementResults, totalRemainingAfterThis, isComplete);
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                processed_conversion: {
                    email: conversionToProcess.email,
                    timestamp: conversionToProcess.timestamp,
                    original_attribution: conversionToProcess.landing_page || null,
                    improvement_found: improvementResults.matchFound,
                    new_attribution: improvementResults.newAttribution || null,
                    improvement_type: improvementResults.improvementType
                },
                results: improvementResults,
                message: statusMessage,
                progress: {
                    total_conversions: allConversions.length,
                    already_processed: totalProcessedAfterThis - 1,
                    just_processed: conversionToProcess.email,
                    remaining_conversions: totalRemainingAfterThis,
                    status: isComplete ? 'COMPLETE' : 'CONTINUE',
                    next_action: isComplete ? 'All conversions optimized!' : 'Run again to process next conversion'
                },
                date_range: 'Past 7 days (safe range)',
                update_result: updateResult
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

// Fetch analytics data for past 7 days (dynamic date range)
async function fetchAnalyticsDataPast7Days() {
    console.log('📊 Fetching analytics data for past 7 days...');
    
    // Calculate dynamic date range - using 7 days to stay within available data
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 7);
    
    // Ensure we don't go before June 11, 2025 (when data starts)
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
    
    console.log(`📡 API Request URL: ${apiUrl}`);
    
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
    
    if (data.page_views && data.page_views.length > 0) {
        const ipv4Count = data.page_views.filter(pv => pv.ip_address && !pv.ip_address.includes(':')).length;
        const ipv6Count = data.page_views.filter(pv => pv.ip_address && pv.ip_address.includes(':')).length;
        console.log(`🌐 IP Address breakdown - IPv4: ${ipv4Count}, IPv6: ${ipv6Count}`);
    }
    
    return data;
}

// Get ALL conversions (not just unattributed)
function getAllConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    // Sort by timestamp DESCENDING (newest first for priority processing)
    const sortedConversions = conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`📊 Found ${sortedConversions.length} total conversions for attribution improvement`);
    console.log('📋 All conversions (newest first):');
    sortedConversions.slice(0, 5).forEach((conv, index) => {
        const attribution = conv.landing_page ? `${conv.landing_page}` : 'NO ATTRIBUTION';
        console.log(`   ${index + 1}. ${conv.email} | ${conv.timestamp} | ${attribution}`);
    });
    if (sortedConversions.length > 5) {
        console.log(`   ... and ${sortedConversions.length - 5} more`);
    }
    
    return sortedConversions;
}

// Filter out conversions marked as "newly updated"
async function filterNewlyUpdatedConversions(allConversions) {
    console.log(`🔍 Checking "newly updated" status for ${allConversions.length} conversions...`);
    
    const unprocessedConversions = [];
    let newlyUpdatedCount = 0;
    
    for (const conversion of allConversions) {
        const newlyUpdatedKey = `newly_updated:${conversion.email}:${conversion.timestamp}`;
        
        try {
            const updatedData = await redisRequest('get', newlyUpdatedKey);
            
            if (updatedData) {
                newlyUpdatedCount++;
                console.log(`✅ Already processed: ${conversion.email}`);
            } else {
                unprocessedConversions.push(conversion);
                console.log(`🔄 Needs processing: ${conversion.email}`);
            }
        } catch (error) {
            // If we can't check status, assume unprocessed
            console.log(`⚠️ Could not check status for ${conversion.email}, assuming unprocessed`);
            unprocessedConversions.push(conversion);
        }
    }
    
    console.log(`📊 Filter complete: ${newlyUpdatedCount} already processed, ${unprocessedConversions.length} need processing`);
    return unprocessedConversions;
}

// Analyze single conversion for attribution improvement
async function analyzeConversionForAttribution(conversion, pageviews) {
    console.log('🔬 ATTRIBUTION ANALYSIS: Finding best temporal match for conversion...');
    
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
            geographic_lookups: 0,
            cache_performance: {}
        }
    };
    
    console.log(`🔍 ANALYZING: ${conversion.email}`);
    console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
    console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
    console.log(`   📄 Current Attribution: ${conversion.landing_page || 'NONE'}`);
    
    // Find all pageviews in 24-hour window before conversion
    const candidatePageviews = findPageviewsIn24HourWindow(conversion, pageviews);
    
    if (candidatePageviews.length === 0) {
        console.log(`   ❌ No pageviews found in 24-hour window before conversion`);
        results.analysis.pageviews_in_window = 0;
        return results;
    }
    
    console.log(`   📱 Found ${candidatePageviews.length} pageviews in 24-hour window`);
    results.analysis.pageviews_in_window = candidatePageviews.length;
    
    // Get conversion geographic data
    console.log(`   🌍 Looking up conversion location...`);
    const conversionGeoData = await getIPLocationData(conversion.ip_address);
    console.log(`   📍 Conversion Location: ${conversionGeoData.city}, ${conversionGeoData.region}, ${conversionGeoData.country} (${conversionGeoData.isp})`);
    
    // Find the best temporal match
    const bestMatch = await findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData);
    
    if (bestMatch) {
        console.log(`   ✅ ATTRIBUTION MATCH FOUND!`);
        
        results.matchFound = true;
        results.newAttribution = bestMatch.pageview.landing_page || bestMatch.pageview.url;
        results.match = bestMatch;
        
        // Determine if we should update
        const shouldUpdate = shouldUpdateAttribution(conversion, bestMatch);
        results.shouldUpdate = shouldUpdate.update;
        results.improvementType = shouldUpdate.type;
        
        console.log(`   🎯 Update Decision: ${shouldUpdate.update ? 'YES' : 'NO'} (${shouldUpdate.type})`);
        
    } else {
        console.log('   ❌ No geographic matches found in 24-hour window');
        results.improvementType = 'NO_MATCH_FOUND';
    }
    
    // Cache performance stats
    const totalLookups = cacheStats.hits + cacheStats.misses + cacheStats.errors;
    const hitRate = totalLookups > 0 ? ((cacheStats.hits / totalLookups) * 100).toFixed(1) : 0;
    
    results.analysis.cache_performance = {
        cache_hits: cacheStats.hits,
        cache_misses: cacheStats.misses,
        cache_hit_rate_percent: hitRate,
        fresh_api_calls: cacheStats.misses
    };
    
    console.log(`📈 ANALYSIS COMPLETE:`);
    console.log(`   💾 Cache hits: ${cacheStats.hits}, misses: ${cacheStats.misses} (${hitRate}% hit rate)`);
    console.log(`   🌍 Fresh API calls: ${cacheStats.misses}`);
    
    return results;
}

// Find pageviews in 24-hour window before conversion
function findPageviewsIn24HourWindow(conversion, pageviews) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - 24 * 60 * 60 * 1000); // 24 hours before
    
    console.log(`   🕐 24-hour window: ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
    
    const candidatePageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address; // Must have IP address
    });
    
    // Sort by timestamp DESCENDING (newest first = closest to conversion)
    candidatePageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    const ipv4Count = candidatePageviews.filter(pv => !pv.ip_address.includes(':')).length;
    const ipv6Count = candidatePageviews.filter(pv => pv.ip_address.includes(':')).length;
    
    console.log(`   📊 Found ${candidatePageviews.length} pageviews in window (IPv4: ${ipv4Count}, IPv6: ${ipv6Count})`);
    console.log(`   ⏰ Sorted by timestamp (newest first) for optimal temporal matching`);
    
    return candidatePageviews;
}

// Find best temporal match (closest to conversion time)
async function findBestTemporalMatch(conversion, candidatePageviews, conversionGeoData) {
    console.log(`   🔍 TEMPORAL MATCHING: Checking pageviews for geographic correlation...`);
    console.log(`   ⚡ Strategy: First acceptable match wins (prioritizing recency)`);
    
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        const ipType = pageview.ip_address.includes(':') ? 'IPv6' : 'IPv4';
        
        console.log(`   🌐 Checking ${i + 1}/${candidatePageviews.length}: ${pageview.ip_address} (${ipType})`);
        console.log(`      ⏰ ${timeDiff.toFixed(1)} min before conversion`);
        console.log(`      📄 Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // Get pageview geographic data
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        console.log(`      📍 Location: ${pageviewGeoData.city}, ${pageviewGeoData.region}, ${pageviewGeoData.country} (${pageviewGeoData.isp})`);
        
        // Compare geographic data
        const geoMatch = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`      ✅ TEMPORAL MATCH FOUND! (${geoMatch.confidence}, Score: ${geoMatch.score})`);
            console.log(`         🎯 Match: City: ${geoMatch.cityMatch ? '✓' : '✗'} | Region: ${geoMatch.regionMatch ? '✓' : '✗'} | Country: ${geoMatch.countryMatch ? '✓' : '✗'} | ISP: ${geoMatch.ispMatch ? '✓' : '✗'}`);
            console.log(`      🏆 SELECTED: Most recent geographic match (${timeDiff.toFixed(1)} min before conversion)`);
            
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
        } else {
            console.log(`      ❌ No geographic match (${geoMatch.confidence}, Score: ${geoMatch.score})`);
        }
        
        // Progress logging for large datasets
        if ((i + 1) % 50 === 0) {
            console.log(`   📊 Progress: ${i + 1}/${candidatePageviews.length} checked`);
        }
    }
    
    console.log(`   ❌ No geographic matches found in any of ${candidatePageviews.length} pageviews`);
    return null;
}

// Determine if attribution should be updated
function shouldUpdateAttribution(conversion, newMatch) {
    const hasCurrentAttribution = conversion.landing_page && conversion.landing_page !== '';
    const newAttribution = newMatch.pageview.landing_page || newMatch.pageview.url;
    
    if (!hasCurrentAttribution) {
        // No current attribution - always create new
        console.log(`   🆕 CREATE: No existing attribution, adding new attribution`);
        return { update: true, type: 'NEW_ATTRIBUTION' };
    }
    
    // Has existing attribution - check temporal precedence
    const currentAttributionTime = conversion.attributed_pageview_timestamp || conversion.timestamp;
    const newMatchTime = newMatch.pageview.timestamp;
    
    if (new Date(newMatchTime) < new Date(currentAttributionTime)) {
        // New match occurred before current attribution
        console.log(`   ⬆️ IMPROVE: New match occurred before current attribution`);
        console.log(`      📅 Current: ${currentAttributionTime}`);
        console.log(`      📅 New: ${newMatchTime}`);
        return { update: true, type: 'TEMPORAL_IMPROVEMENT' };
    } else {
        // Current attribution is already earlier
        console.log(`   ✋ KEEP: Current attribution occurred before new match`);
        console.log(`      📅 Current: ${currentAttributionTime} (keeping)`);
        console.log(`      📅 New: ${newMatchTime} (skipping)`);
        return { update: false, type: 'NO_IMPROVEMENT' };
    }
}

// Mark conversion as newly updated
async function markConversionAsNewlyUpdated(conversion) {
    console.log(`📝 Marking ${conversion.email} as newly updated...`);
    
    try {
        const newlyUpdatedKey = `newly_updated:${conversion.email}:${conversion.timestamp}`;
        const updatedData = {
            email: conversion.email,
            timestamp: conversion.timestamp,
            processed_at: new Date().toISOString(),
            system: 'attribution_improvement'
        };
        
        // Set with 30-day expiration
        await redisRequest('setex', newlyUpdatedKey, 2592000, JSON.stringify(updatedData)); // 30 days
        console.log(`✅ Marked as newly updated: ${conversion.email}`);
    } catch (error) {
        console.log(`⚠️ Could not mark ${conversion.email} as newly updated: ${error.message}`);
    }
}

// Generate status message
function generateStatusMessage(conversion, results, remainingCount, isComplete) {
    const email = conversion.email;
    
    if (isComplete) {
        return `Attribution improvement COMPLETE! Processed final conversion: ${email}. All recent conversions have been optimized.`;
    }
    
    if (results.shouldUpdate) {
        const type = results.improvementType === 'NEW_ATTRIBUTION' ? 'NEW attribution created' : 'Attribution IMPROVED';
        return `${type} for ${email}: ${results.newAttribution}. ${remainingCount} conversions remaining - run again to continue.`;
    } else if (results.matchFound) {
        return `Analyzed ${email}: Current attribution is already optimal. ${remainingCount} conversions remaining - run again to continue.`;
    } else {
        return `Analyzed ${email}: No attribution matches found in 24-hour window. ${remainingCount} conversions remaining - run again to continue.`;
    }
}

// Update conversion attribution in Redis
async function updateConversionAttribution(conversion, improvementResults) {
    console.log(`📝 Updating attribution for ${conversion.email}...`);
    
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
                    method: 'temporal_optimization',
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
            
            console.log(`✅ Successfully updated ${conversion.email}`);
            console.log(`   📄 New attribution: ${improvementResults.newAttribution}`);
            console.log(`   🕐 Pageview time: ${improvementResults.match.pageview.timestamp}`);
            
            return {
                success: true,
                updated_attribution: improvementResults.newAttribution,
                previous_attribution: conversionData.landing_page || null,
                improvement_type: improvementResults.improvementType
            };
            
        } else {
            console.log(`⚠️ Could not find Redis key for ${conversion.email}`);
            return { success: false, error: 'Conversion not found in Redis' };
        }
        
    } catch (error) {
        console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        return { success: false, error: error.message };
    }
}

// NEW: Get cached geographic data using SAME cache structure as track.js
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

// Get location/ISP data using cache
async function getIPLocationData(ip) {
    // Check cache first
    const cached = await getCachedGeoData(ip);
    if (cached) return cached;
    
    // Make fresh API call
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
            
            // Cache the result
            try {
                const cacheKey = `geo_cache:${ip.replace(/:/g, '_')}`;
                const encodedData = encodeURIComponent(JSON.stringify(geoData));
                await redisRequest('setex', cacheKey, 86400, encodedData);
            } catch (cacheError) {
                console.log(`   ⚠️ Failed to cache geo data for ${ip}`);
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

// Extract best ISP info
function extractBestISP(data) {
    if (data.company?.name) return data.company.name;
    if (data.asn?.name) return data.asn.name;
    if (data.org) return data.org;
    if (data.carrier?.name) return data.carrier.name;
    return 'Unknown';
}

// Compare geographic data
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
        isMatch = true;
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

// Compare ISP names
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

// Redis request helper
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

// Find the Redis key for a specific conversion
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
        
        // Create new key if not found
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for ${conversion.email}:`, error);
        return null;
    }
}
