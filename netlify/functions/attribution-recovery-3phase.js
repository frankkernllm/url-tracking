exports.handler = async (event, context) => {
    // Batch Processing Attribution Recovery Function - processes conversions in small batches
    // to avoid timeout issues with larger datasets while maintaining identical attribution quality.
    // Run multiple times until all conversions are processed.
    
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
        console.log('🎯 Starting Batch Attribution Recovery (Past 24 Hours)');
        
        // BATCH CONFIGURATION - Start with 1 conversion, increase after testing
        const BATCH_SIZE = 1; // TODO: Test with 1, then try 2, 3 to find optimal size
        
        // Step 1: Fetch analytics data from past 24 hours (unchanged)
        const analyticsData = await fetchAnalyticsData();
        
        // Step 2: Find unattributed conversions (unchanged)
        const allUnattributedConversions = findUnattributedConversions(analyticsData.conversions);
        
        if (allUnattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found',
                    results: { total: 0, recovered: 0, phases: {} },
                    batch_info: { processed: 0, remaining: 0, batch_size: BATCH_SIZE }
                })
            };
        }
        
        // Step 2.5: BATCH PROCESSING - Filter to unprocessed conversions only
        const unprocessedConversions = await filterUnprocessedConversions(allUnattributedConversions);
        
        if (unprocessedConversions.length === 0) {
            console.log('✅ All conversions have been processed in previous runs');
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'All conversions already processed',
                    results: { total: allUnattributedConversions.length, recovered: 0, phases: {} },
                    batch_info: { 
                        processed: allUnattributedConversions.length, 
                        remaining: 0, 
                        batch_size: BATCH_SIZE,
                        status: 'COMPLETE'
                    }
                })
            };
        }
        
        // Step 2.6: Take only the batch size from unprocessed conversions
        const conversionsToProcess = unprocessedConversions.slice(0, BATCH_SIZE);
        const remainingAfterBatch = unprocessedConversions.length - conversionsToProcess.length;
        
        console.log(`📦 BATCH PROCESSING: Processing ${conversionsToProcess.length} conversions (${remainingAfterBatch} remaining)`);
        console.log(`📊 Total Status: ${allUnattributedConversions.length} total unattributed, ${unprocessedConversions.length} unprocessed`);
        
        // Step 3: Analyze conversions in this batch (IDENTICAL logic to original)
        const recoveryResults = await analyzeUnattributedConversions(conversionsToProcess, analyticsData.page_views);
        
        // Step 4: Update Redis with recovered attributions (unchanged)
        if (recoveryResults.matches.length > 0) {
            console.log(`📝 Updating ${recoveryResults.matches.length} recovered attributions in Redis...`);
            try {
                await updateRecoveredAttributions(recoveryResults.matches);
            } catch (redisError) {
                console.error('❌ Redis update failed but recovery succeeded:', redisError);
            }
        }
        
        // Step 5: Mark processed conversions to prevent re-processing
        await markConversionsAsProcessed(conversionsToProcess);
        
        // Step 6: Return batch processing status
        const batchComplete = remainingAfterBatch === 0;
        const statusMessage = batchComplete ? 
            `Batch processing COMPLETE: ${recoveryResults.recovered}/${recoveryResults.total} conversions recovered in final batch` :
            `Batch ${conversionsToProcess.length}/${unprocessedConversions.length} complete: ${recoveryResults.recovered}/${recoveryResults.total} recovered. ${remainingAfterBatch} conversions remaining - run again to continue.`;
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: recoveryResults,
                message: statusMessage,
                batch_info: {
                    processed_this_batch: conversionsToProcess.length,
                    recovered_this_batch: recoveryResults.recovered,
                    remaining_conversions: remainingAfterBatch,
                    batch_size: BATCH_SIZE,
                    status: batchComplete ? 'COMPLETE' : 'CONTINUE',
                    next_action: batchComplete ? 'All done!' : 'Run the function again to process remaining conversions'
                }
            })
        };

    } catch (error) {
        console.error('❌ Batch recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Batch recovery failed',
                details: error.message
            })
        };
    }
};

// NEW: Filter out conversions that have already been processed
async function filterUnprocessedConversions(conversions) {
    console.log('🔍 Checking which conversions have already been processed...');
    
    const unprocessed = [];
    
    for (const conversion of conversions) {
        const isProcessed = await isConversionProcessed(conversion);
        if (!isProcessed) {
            unprocessed.push(conversion);
        } else {
            console.log(`⏭️  Skipping already processed: ${conversion.email}`);
        }
    }
    
    console.log(`📊 Filtered results: ${unprocessed.length} unprocessed out of ${conversions.length} total unattributed`);
    return unprocessed;
}

// NEW: Check if a conversion has been processed in a previous batch
async function isConversionProcessed(conversion) {
    try {
        const processedKey = `processed_conversion:${conversion.email}:${conversion.timestamp}`;
        const result = await redisRequest('get', processedKey);
        return result !== null;
    } catch (error) {
        console.log(`⚠️ Could not check processed status for ${conversion.email}: ${error.message}`);
        return false; // If we can't check, assume not processed to be safe
    }
}

// NEW: Mark conversions as processed to prevent re-processing
async function markConversionsAsProcessed(conversions) {
    console.log(`📝 Marking ${conversions.length} conversions as processed...`);
    
    for (const conversion of conversions) {
        try {
            const processedKey = `processed_conversion:${conversion.email}:${conversion.timestamp}`;
            const processedData = {
                email: conversion.email,
                timestamp: conversion.timestamp,
                processed_at: new Date().toISOString(),
                batch_id: Date.now()
            };
            
            // Set with 7-day expiration to prevent Redis bloat
            await redisRequest('setex', processedKey, 604800, JSON.stringify(processedData)); // 7 days = 604800 seconds
            console.log(`✅ Marked as processed: ${conversion.email}`);
        } catch (error) {
            console.log(`⚠️ Could not mark ${conversion.email} as processed: ${error.message}`);
        }
    }
}

// Step 1: Fetch analytics data from past 24 hours (UNCHANGED - identical to original)
async function fetchAnalyticsData() {
    console.log('📊 Fetching analytics data for past 24 hours...');
    
    // Calculate past 24 hours dynamically
    const now = new Date();
    const endDate = now.toISOString().split('T')[0]; // Today's date YYYY-MM-DD
    const yesterdayDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const startDate = yesterdayDate.toISOString().split('T')[0]; // Yesterday's date YYYY-MM-DD
    
    console.log(`📅 Date range: ${startDate} to ${endDate} (past 24 hours)`);
    
    const params = new URLSearchParams();
    params.append('start_date', startDate);
    params.append('end_date', endDate);
    
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
    console.log(`✅ Analytics data loaded for ${startDate} to ${endDate}:`);
    console.log(`   📊 Total conversions: ${data.conversions?.length || 0}`);
    console.log(`   📊 Total pageviews: ${data.page_views?.length || 0}`);
    
    if (data.page_views && data.page_views.length > 0) {
        const ipv4Count = data.page_views.filter(pv => pv.ip_address && !pv.ip_address.includes(':')).length;
        const ipv6Count = data.page_views.filter(pv => pv.ip_address && pv.ip_address.includes(':')).length;
        console.log(`🌐 IP Address breakdown - IPv4: ${ipv4Count}, IPv6: ${ipv6Count}`);
    }
    
    return data;
}

// Step 2: Find unattributed conversions (UNCHANGED - identical to original)
function findUnattributedConversions(conversions) {
    if (!conversions || conversions.length === 0) {
        console.log('❌ No conversions found in analytics data');
        return [];
    }
    
    const unattributed = conversions.filter(conv => 
        conv.attribution_found === false || 
        !conv.landing_page || 
        conv.landing_page === ''
    );
    
    console.log(`🚨 Found ${unattributed.length} unattributed conversions out of ${conversions.length} total`);
    
    if (unattributed.length > 0) {
        console.log('📋 Unattributed conversions:');
        unattributed.forEach((conv, index) => {
            console.log(`   ${index + 1}. ${conv.email} | ${conv.ip_address} | ${conv.timestamp}`);
        });
    }
    
    return unattributed;
}

// Step 3: Analyze unattributed conversions with FOUR phases (UNCHANGED - identical to original)
async function analyzeUnattributedConversions(unattributedConversions, pageviews) {
    console.log('🔬 Analyzing unattributed conversions from past 24 hours for IPv6 pageview matches...');
    
    const results = {
        total: unattributedConversions.length,
        recovered: 0,
        matches: [],
        phases: {
            'Phase 1': { attempts: 0, matches: 0 },
            'Phase 2': { attempts: 0, matches: 0 },
            'Phase 3': { attempts: 0, matches: 0 },
            'Phase 4': { attempts: 0, matches: 0 }
        }
    };
    
    for (let i = 0; i < unattributedConversions.length; i++) {
        const conversion = unattributedConversions[i];
        console.log(`🔍 ANALYZING CONVERSION ${i + 1}/${unattributedConversions.length}: ${conversion.email}`);
        console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
        
        // Try four phases in sequence (balanced timeline for remaining conversions)
        const phases = [
            { name: 'Phase 1', start: 0, end: 15, confidence: 'HIGH' },
            { name: 'Phase 2', start: 15, end: 45, confidence: 'MEDIUM' },
            { name: 'Phase 3', start: 45, end: 120, confidence: 'EXTENDED' },
            { name: 'Phase 4', start: 120, end: 180, confidence: 'DEEP_HISTORY_3H' }
        ];
        
        let matched = false;
        
        for (const phase of phases) {
            if (matched) break;
            
            results.phases[phase.name].attempts++;
            
            if (phase.name === 'Phase 4') {
                console.log(`   🕐 ${phase.name}: Searching ${phase.start}-${phase.end} minutes (2-3 hours before conversion)`);
            } else {
                console.log(`   🕐 ${phase.name}: Searching ${phase.start}-${phase.end} minute window`);
            }
            
            // Find IPv6 pageviews in window
            const candidatePageviews = findIPv6PageviewsInWindow(conversion, pageviews, phase.start, phase.end);
            
            if (candidatePageviews.length === 0) {
                console.log(`   ❌ No IPv6 pageviews found in ${phase.name} window`);
                continue;
            }
            
            console.log(`   📱 Found ${candidatePageviews.length} IPv6 pageviews in ${phase.name} window`);
            
            // Get geographic data for conversion IP
            const conversionGeoData = await getIPLocationData(conversion.ip_address);
            console.log(`   📍 Conversion Location: ${conversionGeoData.city}, ${conversionGeoData.region}, ${conversionGeoData.country} (${conversionGeoData.isp})`);
            
            // Check each IPv6 pageview for geographic match
            const match = await checkIPv6Candidates(conversion, candidatePageviews, conversionGeoData);
            
            if (match) {
                console.log(`   ✅ ${phase.name} MATCH FOUND!`);
                
                results.matches.push({
                    conversion: conversion,
                    match: match,
                    phase: phase.name,
                    confidence: phase.confidence
                });
                
                results.phases[phase.name].matches++;
                results.recovered++;
                matched = true;
            }
        }
        
        if (!matched) {
            console.log('   ❌ No matches found in any phase');
        }
    }
    
    console.log(`🏁 Recovery complete: ${results.recovered}/${results.total} conversions recovered`);
    return results;
}

// Find IPv6 pageviews within time window (UNCHANGED - identical to original)
function findIPv6PageviewsInWindow(conversion, pageviews, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    console.log(`   🕐 Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
    
    const ipv6Pageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address && pv.ip_address.includes(':'); // IPv6 addresses contain colons
    });
    
    console.log(`   📊 Found ${ipv6Pageviews.length} IPv6 pageviews in time window out of ${pageviews.length} total pageviews`);
    
    return ipv6Pageviews;
}

// Get location/ISP data for an IP using IPInfo.io (UNCHANGED - identical to original)
async function getIPLocationData(ip) {
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    const url = `https://ipinfo.io/${ip}?token=${token}`;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        if (response.ok) {
            const data = await response.json();
            
            return {
                ip: data.ip,
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                timezone: data.timezone || 'Unknown',
                location: data.loc || '0,0'
            };
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        console.log(`   ⚠️  Failed to lookup ${ip}: ${error.message}`);
        return {
            ip: ip,
            city: 'LOOKUP_FAILED',
            region: 'LOOKUP_FAILED',
            country: 'LOOKUP_FAILED',
            isp: 'LOOKUP_FAILED'
        };
    }
}

// Extract best ISP info (UNCHANGED - identical to original)
function extractBestISP(data) {
    if (data.company && data.company.name) {
        return data.company.name;
    } else if (data.asn && data.asn.name) {
        return data.asn.name;
    } else if (data.org) {
        return data.org;
    } else if (data.carrier && data.carrier.name) {
        return data.carrier.name;
    }
    return 'Unknown';
}

// Check IPv6 candidates against conversion for geographic matches (UNCHANGED - identical to original)
async function checkIPv6Candidates(conversion, candidatePageviews, conversionGeoData) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        console.log(`   🌈 IPv6 Candidate ${i + 1}: ${pageview.ip_address}`);
        console.log(`      ⏰ Pageview Time: ${pageview.timestamp} (${timeDiff.toFixed(1)} min before)`);
        console.log(`      📄 Landing Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // Get geographic data for IPv6 pageview
        const pageviewGeoData = await getIPLocationData(pageview.ip_address);
        console.log(`      📍 IPv6 Location: ${pageviewGeoData.city}, ${pageviewGeoData.region}, ${pageviewGeoData.country} (${pageviewGeoData.isp})`);
        
        // Compare geographic data
        const match = compareGeographicData(conversionGeoData, pageviewGeoData);
        
        if (match.isMatch) {
            console.log(`      ✅ GEOGRAPHIC MATCH FOUND! (${match.confidence})`);
            console.log(`         🎯 City: ${match.cityMatch ? '✓' : '✗'} | Region: ${match.regionMatch ? '✓' : '✗'} | Country: ${match.countryMatch ? '✓' : '✗'} | ISP: ${match.ispMatch ? '✓' : '✗'}`);
            
            return {
                pageview: pageview,
                score: match.score,
                timeDiff: timeDiff,
                confidence: match.confidence,
                conversionGeo: conversionGeoData,
                pageviewGeo: pageviewGeoData
            };
        } else {
            console.log(`      ❌ No geographic match (${match.confidence})`);
        }
    }
    
    return null;
}

// Compare geographic data between conversion and pageview (UNCHANGED - identical to original)
function compareGeographicData(conversionGeo, pageviewGeo) {
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    // Scoring system
    let score = 0;
    if (cityMatch) score += 3;
    if (regionMatch) score += 2;
    if (countryMatch) score += 1;
    if (ispMatch) score += 2;

    // Determine confidence level
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

// Compare ISP names (UNCHANGED - identical to original)
function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = (str) => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    // Exact match
    if (norm1 === norm2) return true;
    
    // Contains match
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    // ASN match
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

// Helper function to make Redis HTTP requests (ENHANCED - added SETEX support for expiration)
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
    // For complex commands like SET with JSON, use POST with body
    if ((command.toLowerCase() === 'set' || command.toLowerCase() === 'setex') && args.length >= 2) {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify([command, ...args])
        });
        
        if (!response.ok) {
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
    } 
    // For simple commands like GET, KEYS, use URL path
    else {
        const response = await fetch(`${url}/${command}/${args.join('/')}`, {
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json'
            }
        });
        
        if (!response.ok) {
            throw new Error(`Redis request failed: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        return data.result;
    }
}

// Update recovered attributions in Redis (UNCHANGED - identical to original)
async function updateRecoveredAttributions(matches) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis...`);
    
    for (const match of matches) {
        const conversion = match.conversion;
        const pageview = match.match.pageview;
        
        try {
            console.log(`🔄 Updating ${conversion.email}...`);
            
            // Try to find the conversion record in Redis by searching different key patterns
            const conversionKey = await findConversionKey(conversion);
            
            if (conversionKey) {
                // Get the existing conversion data
                const existingData = await redisRequest('get', conversionKey);
                let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
                
                // Update with recovered attribution
                const updatedConversion = {
                    ...conversionData,
                    attribution_found: true,
                    landing_page: pageview.landing_page || pageview.url,
                    source: pageview.source || 'recovered',
                    utm_campaign: pageview.utm_campaign || conversionData.utm_campaign,
                    utm_medium: pageview.utm_medium || conversionData.utm_medium,
                    referrer_url: pageview.referrer_url || conversionData.referrer_url,
                    recovery_method: 'four_phase_geographic',
                    recovery_phase: match.phase,
                    recovery_confidence: match.confidence,
                    recovery_score: match.match.score,
                    recovery_timestamp: new Date().toISOString(),
                    recovery_ipv6_match: pageview.ip_address
                };
                
                // Save back to Redis (using fixed POST method)
                await redisRequest('set', conversionKey, JSON.stringify(updatedConversion));
                
                console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase})`);
            } else {
                console.log(`⚠️ Could not find Redis key for ${conversion.email}`);
            }
            
        } catch (error) {
            console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        }
    }
    
    console.log(`📝 Redis update complete for ${matches.length} attributions`);
}

// Find the Redis key for a specific conversion (UNCHANGED - identical to original)
async function findConversionKey(conversion) {
    try {
        // Try different possible key patterns (including the actual format seen in logs)
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
                    // If multiple keys, try to find the exact match
                    for (const key of keys) {
                        const data = await redisRequest('get', key);
                        if (data) {
                            const parsed = typeof data === 'string' ? JSON.parse(data) : data;
                            if (parsed.email === conversion.email && 
                                Math.abs(new Date(parsed.timestamp) - new Date(conversion.timestamp)) < 60000) {
                                console.log(`🔍 Found conversion key: ${key}`);
                                return key;
                            }
                        }
                    }
                }
            } catch (error) {
                // Continue trying other patterns
                console.log(`   ⚠️ Pattern ${pattern} failed: ${error.message}`);
            }
        }
        
        // If no existing key found, create a new one
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        console.log(`🆕 Creating new conversion key: ${newKey}`);
        
        // Store the conversion data first
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for ${conversion.email}:`, error);
        return null;
    }
}
