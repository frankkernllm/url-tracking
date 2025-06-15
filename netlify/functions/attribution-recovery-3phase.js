exports.handler = async (event, context) => {
    // BACK TO ORIGINAL LOGIC with only timeout monitoring and API rate limit handling
    
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
        console.log('🎯 Starting Four-Phase Attribution Recovery (Past 24 Hours)');
        
        // Add timeout monitoring
        const startTime = Date.now();
        const timeLimit = (context.getRemainingTimeInMillis ? context.getRemainingTimeInMillis() : 300000) - 30000; // 30s buffer
        console.log(`⏱️ Time limit: ${Math.round(timeLimit/1000)} seconds`);
        
        // Step 1: Fetch analytics data from past 24 hours
        const analyticsData = await fetchAnalyticsData();
        
        // Step 2: Find unattributed conversions
        const unattributedConversions = findUnattributedConversions(analyticsData.conversions);
        
        if (unattributedConversions.length === 0) {
            return {
                statusCode: 200,
                headers,
                body: JSON.stringify({
                    success: true,
                    message: 'No unattributed conversions found',
                    results: { total: 0, recovered: 0, phases: {} }
                })
            };
        }
        
        // Step 3: Analyze unattributed conversions (ORIGINAL LOGIC with timeout monitoring)
        const recoveryResults = await analyzeUnattributedConversionsWithTimeout(
            unattributedConversions, 
            analyticsData.page_views,
            startTime,
            timeLimit
        );
        
        // Step 4: Update Redis with recovered attributions
        if (recoveryResults.matches.length > 0) {
            console.log(`📝 Updating ${recoveryResults.matches.length} recovered attributions in Redis...`);
            try {
                await updateRecoveredAttributions(recoveryResults.matches);
            } catch (redisError) {
                console.error('❌ Redis update failed but recovery succeeded:', redisError);
            }
        }
        
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: recoveryResults,
                message: `Recovery complete: ${recoveryResults.recovered}/${recoveryResults.total} conversions recovered`
            })
        };

    } catch (error) {
        console.error('❌ Recovery error:', error);
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({
                error: 'Recovery failed',
                details: error.message
            })
        };
    }
};

// MODIFIED: Original analysis logic with timeout monitoring
async function analyzeUnattributedConversionsWithTimeout(unattributedConversions, pageviews, startTime, timeLimit) {
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
        },
        timeouts: 0
    };
    
    for (let i = 0; i < unattributedConversions.length; i++) {
        const conversion = unattributedConversions[i];
        
        // Check if we're running out of time
        const elapsed = Date.now() - startTime;
        const remaining = timeLimit - elapsed;
        
        if (remaining < 60000) { // Stop if less than 60 seconds remaining
            console.log(`⏰ Stopping early due to time limit. Processed ${i}/${unattributedConversions.length} conversions`);
            results.timeouts = unattributedConversions.length - i;
            break;
        }
        
        console.log(`🔍 ANALYZING CONVERSION ${i + 1}/${unattributedConversions.length}: ${conversion.email}`);
        console.log(`   📍 Conversion IP: ${conversion.ip_address}`);
        console.log(`   ⏰ Conversion Time: ${conversion.timestamp}`);
        console.log(`   ⏱️ Time remaining: ${Math.round(remaining/1000)}s`);
        
        // Try four phases in sequence (ORIGINAL LOGIC)
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
            
            // Find IPv6 pageviews in window (ORIGINAL LOGIC)
            const candidatePageviews = findIPv6PageviewsInWindow(conversion, pageviews, phase.start, phase.end);
            
            if (candidatePageviews.length === 0) {
                console.log(`   ❌ No IPv6 pageviews found in ${phase.name} window`);
                continue;
            }
            
            console.log(`   📱 Found ${candidatePageviews.length} IPv6 pageviews in ${phase.name} window`);
            
            // Get geographic data for conversion IP
            const conversionGeoData = await getIPLocationDataWithRetry(conversion.ip_address);
            console.log(`   📍 Conversion Location: ${conversionGeoData.city}, ${conversionGeoData.region}, ${conversionGeoData.country} (${conversionGeoData.isp})`);
            
            // Check each IPv6 pageview for geographic match (ORIGINAL LOGIC)
            const match = await checkIPv6CandidatesWithRateLimit(conversion, candidatePageviews, conversionGeoData);
            
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

// MODIFIED: Add retry logic and rate limit handling to IP lookups
async function getIPLocationDataWithRetry(ip) {
    const maxRetries = 3;
    let attempt = 0;
    
    while (attempt < maxRetries) {
        try {
            const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
            const url = `https://ipinfo.io/${ip}?token=${token}`;
            
            const response = await fetch(url, {
                method: 'GET',
                headers: { 'Accept': 'application/json' },
                timeout: 10000 // 10 second timeout
            });
            
            if (response.status === 429) {
                // Rate limited - wait and retry
                const delay = Math.pow(2, attempt) * 1000; // Exponential backoff
                console.log(`   ⏳ Rate limited, waiting ${delay}ms before retry ${attempt + 1}/${maxRetries}`);
                await new Promise(resolve => setTimeout(resolve, delay));
                attempt++;
                continue;
            }
            
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
            attempt++;
            if (attempt >= maxRetries) {
                console.log(`   ⚠️  Failed to lookup ${ip} after ${maxRetries} attempts: ${error.message}`);
                return {
                    ip: ip,
                    city: 'LOOKUP_FAILED',
                    region: 'LOOKUP_FAILED',
                    country: 'LOOKUP_FAILED',
                    isp: 'LOOKUP_FAILED'
                };
            } else {
                console.log(`   ⚠️  Attempt ${attempt} failed for ${ip}, retrying: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
            }
        }
    }
}

// MODIFIED: Add rate limiting between IP lookups
async function checkIPv6CandidatesWithRateLimit(conversion, candidatePageviews, conversionGeoData) {
    // ORIGINAL LOGIC: Check ALL candidates
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        console.log(`   🌈 IPv6 Candidate ${i + 1}: ${pageview.ip_address}`);
        console.log(`      ⏰ Pageview Time: ${pageview.timestamp} (${timeDiff.toFixed(1)} min before)`);
        console.log(`      📄 Landing Page: ${pageview.landing_page || pageview.url || 'Unknown'}`);
        
        // Get geographic data for IPv6 pageview with rate limiting
        const pageviewGeoData = await getIPLocationDataWithRetry(pageview.ip_address);
        console.log(`      📍 IPv6 Location: ${pageviewGeoData.city}, ${pageviewGeoData.region}, ${pageviewGeoData.country} (${pageviewGeoData.isp})`);
        
        // Compare geographic data (ORIGINAL LOGIC)
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
        
        // Small delay between IP lookups to avoid rate limiting
        if (i < candidatePageviews.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 100)); // 100ms delay
        }
    }
    
    return null;
}

// ALL OTHER FUNCTIONS REMAIN EXACTLY THE SAME AS ORIGINAL

async function fetchAnalyticsData() {
    console.log('📊 Fetching analytics data for past 24 hours...');
    
    const now = new Date();
    const endDate = now.toISOString().split('T')[0];
    const yesterdayDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const startDate = yesterdayDate.toISOString().split('T')[0];
    
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

function findIPv6PageviewsInWindow(conversion, pageviews, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    console.log(`   🕐 Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
    
    const ipv6Pageviews = pageviews.filter(pv => {
        const pvTime = new Date(pv.timestamp);
        return pvTime >= windowStart && 
               pvTime <= conversionTime && 
               pv.ip_address && pv.ip_address.includes(':');
    });
    
    console.log(`   📊 Found ${ipv6Pageviews.length} IPv6 pageviews in time window out of ${pageviews.length} total pageviews`);
    
    return ipv6Pageviews;
}

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
    
    if (command.toLowerCase() === 'set' && args.length === 2) {
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
    } else {
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

async function updateRecoveredAttributions(matches) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis...`);
    
    for (const match of matches) {
        const conversion = match.conversion;
        const pageview = match.match.pageview;
        
        try {
            console.log(`🔄 Updating ${conversion.email}...`);
            
            const conversionKey = await findConversionKey(conversion);
            
            if (conversionKey) {
                const existingData = await redisRequest('get', conversionKey);
                let conversionData = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
                
                const updatedConversion = {
                    ...conversionData,
                    attribution_found: true,
                    landing_page: pageview.landing_page || pageview.url,
                    source: pageview.source || 'recovered',
                    utm_campaign: pageview.utm_campaign || conversionData.utm_campaign,
                    utm_medium: pageview.utm_medium || conversionData.utm_medium,
                    referrer_url: pageview.referrer_url || conversionData.referrer_url,
                    recovery_method: 'four_phase_geographic_rate_limited',
                    recovery_phase: match.phase,
                    recovery_confidence: match.confidence,
                    recovery_score: match.match.score,
                    recovery_timestamp: new Date().toISOString(),
                    recovery_ipv6_match: pageview.ip_address
                };
                
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
                                console.log(`🔍 Found conversion key: ${key}`);
                                return key;
                            }
                        }
                    }
                }
            } catch (error) {
                console.log(`   ⚠️ Pattern ${pattern} failed: ${error.message}`);
            }
        }
        
        const newKey = `conversion_${conversion.email}_${Date.now()}`;
        console.log(`🆕 Creating new conversion key: ${newKey}`);
        
        await redisRequest('set', newKey, JSON.stringify(conversion));
        return newKey;
        
    } catch (error) {
        console.error(`❌ Error finding conversion key for ${conversion.email}:`, error);
        return null;
    }
}
