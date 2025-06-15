// netlify/functions/attribution-recovery-5phase.js
// Fixed version using direct HTTP calls to Upstash Redis REST API with individual processing and 5 phases

exports.handler = async (event, context) => {
    // CORS headers
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Content-Type': 'application/json'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 200, headers, body: '' };
    }

    // API Key validation
    const apiKey = event.headers['x-api-key'];
    const expectedApiKey = process.env.OJOY_API_KEY;
    
    if (!expectedApiKey) {
        return {
            statusCode: 500,
            headers,
            body: JSON.stringify({ error: 'Server configuration error: API key not configured' })
        };
    }
    
    if (apiKey !== expectedApiKey) {
        return {
            statusCode: 401,
            headers,
            body: JSON.stringify({ error: 'Invalid API key' })
        };
    }

    try {
        console.log('🎯 Starting Five-Phase Attribution Recovery with Individual Processing');
        
        // Step 1: Get unattributed conversions
        const unattributedConversions = await getUnattributedConversions();
        
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

        console.log(`📊 Found ${unattributedConversions.length} unattributed conversions`);

        // Step 2: Process unattributed conversions one at a time
        const allResults = {
            total: unattributedConversions.length,
            recovered: 0,
            matches: [],
            phases: {
                'Phase 1': { attempts: 0, matches: 0 },
                'Phase 2': { attempts: 0, matches: 0 },
                'Phase 3': { attempts: 0, matches: 0 },
                'Phase 4': { attempts: 0, matches: 0 },
                'Phase 5': { attempts: 0, matches: 0 }
            },
            conversions: []
        };
        
        console.log(`🔄 Processing ${unattributedConversions.length} conversions individually`);
        
        // Process each conversion individually
        for (let i = 0; i < unattributedConversions.length; i++) {
            const conversion = unattributedConversions[i];
            const conversionNumber = i + 1;
            
            console.log(`\n🔍 Processing Conversion ${conversionNumber}/${unattributedConversions.length}: ${conversion.email}`);
            
            try {
                // Run five-phase recovery for this single conversion
                const conversionResults = await runFivePhaseRecovery([conversion]);
                
                // Update Redis immediately after processing this conversion
                if (conversionResults.matches.length > 0) {
                    console.log(`📝 Updating recovered attribution for ${conversion.email} in Redis...`);
                    await updateRecoveredAttributions(conversionResults.matches);
                }
                
                // Aggregate results
                allResults.recovered += conversionResults.recovered;
                allResults.matches.push(...conversionResults.matches);
                
                // Merge phase statistics
                Object.keys(conversionResults.phases).forEach(phase => {
                    allResults.phases[phase].attempts += conversionResults.phases[phase].attempts;
                    allResults.phases[phase].matches += conversionResults.phases[phase].matches;
                });
                
                allResults.conversions.push({
                    conversionNumber: conversionNumber,
                    email: conversion.email,
                    recovered: conversionResults.recovered,
                    success: true
                });
                
                console.log(`✅ Conversion ${conversionNumber} complete: ${conversionResults.recovered > 0 ? 'RECOVERED' : 'NO MATCH'}`);
                
                // Add a small delay between conversions to prevent API rate limiting
                if (i < unattributedConversions.length - 1) {
                    console.log('⏱️  Waiting 1 second before next conversion...');
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
                
            } catch (conversionError) {
                console.error(`❌ Conversion ${conversionNumber} (${conversion.email}) failed:`, conversionError.message);
                
                allResults.conversions.push({
                    conversionNumber: conversionNumber,
                    email: conversion.email,
                    recovered: 0,
                    success: false,
                    error: conversionError.message
                });
                
                // Continue with next conversion instead of failing entirely
                continue;
            }
        }
        
        console.log(`\n🏁 All conversions complete: ${allResults.recovered}/${allResults.total} total conversions recovered`);

        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({
                success: true,
                results: allResults,
                message: `Individual recovery complete: ${allResults.recovered}/${allResults.total} conversions recovered`
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

// Helper function to make Redis HTTP requests
async function redisRequest(command, ...args) {
    const url = process.env.UPSTASH_REDIS_REST_URL;
    const token = process.env.UPSTASH_REDIS_REST_TOKEN;
    
    if (!url || !token) {
        throw new Error('Missing Redis configuration');
    }
    
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

async function getUnattributedConversions() {
    // Get all conversion keys
    const conversionKeys = await redisRequest('keys', 'conversion_*');
    const unattributed = [];
    
    if (!conversionKeys || conversionKeys.length === 0) {
        console.log('No conversion keys found');
        return [];
    }
    
    // Check each conversion for attribution status
    for (const key of conversionKeys) {
        try {
            const conversionData = await redisRequest('get', key);
            if (conversionData) {
                let conversion;
                try {
                    conversion = typeof conversionData === 'string' ? JSON.parse(conversionData) : conversionData;
                } catch (parseError) {
                    console.log(`Failed to parse conversion data for key ${key}:`, parseError);
                    continue;
                }
                
                // Check if conversion lacks attribution
                if (!conversion.attribution_found || conversion.attribution_found === false) {
                    unattributed.push({
                        ...conversion,
                        key: key
                    });
                }
            }
        } catch (error) {
            console.log(`Error processing conversion key ${key}:`, error);
        }
    }
    
    console.log(`Found ${unattributed.length} unattributed conversions`);
    return unattributed;
}

async function runFivePhaseRecovery(conversions) {
    const phases = [
        { name: 'Phase 1', start: 0, end: 15, confidence: 'HIGH' },
        { name: 'Phase 2', start: 15, end: 45, confidence: 'MEDIUM' },
        { name: 'Phase 3', start: 45, end: 120, confidence: 'EXTENDED' },
        { name: 'Phase 4', start: 120, end: 180, confidence: 'DEEP_SEARCH' },
        { name: 'Phase 5', start: 180, end: 240, confidence: 'MAXIMUM_SEARCH' }
    ];
    
    const results = {
        total: conversions.length,
        recovered: 0,
        matches: [],
        phases: {
            'Phase 1': { attempts: 0, matches: 0 },
            'Phase 2': { attempts: 0, matches: 0 },
            'Phase 3': { attempts: 0, matches: 0 },
            'Phase 4': { attempts: 0, matches: 0 },
            'Phase 5': { attempts: 0, matches: 0 }
        }
    };

    for (const conversion of conversions) {
        console.log(`🔍 Processing: ${conversion.email} (${conversion.ip_address})`);
        
        let matched = false;
        
        // Try each phase in sequence - stop when match found
        for (const phase of phases) {
            if (matched) break;
            
            results.phases[phase.name].attempts++;
            console.log(`   ${phase.name}: Searching ${phase.start}-${phase.end} minutes before conversion...`);
            
            const match = await searchPhase(conversion, phase.start, phase.end);
            
            if (match && match.score >= 3) {
                console.log(`   ✅ ${phase.name} match found! Score: ${match.score}`);
                
                results.matches.push({
                    conversion: conversion,
                    match: match,
                    phase: phase.name,
                    confidence: phase.confidence
                });
                
                results.phases[phase.name].matches++;
                results.recovered++;
                matched = true;
            } else {
                console.log(`   ❌ No match found in ${phase.name}`);
            }
        }
        
        if (!matched) {
            console.log(`   ❌ No matches found in any of the 5 phases`);
        }
    }
    
    return results;
}

async function searchPhase(conversion, startMinutes, endMinutes) {
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - endMinutes * 60 * 1000);
    const windowEnd = new Date(conversionTime.getTime() - startMinutes * 60 * 1000);
    
    console.log(`   🔍 Search window: ${windowStart.toISOString()} to ${windowEnd.toISOString()}`);
    
    // Get all attribution keys (pageviews)
    const attributionKeys = await redisRequest('keys', 'attribution_*');
    const candidates = [];
    
    if (!attributionKeys || attributionKeys.length === 0) {
        console.log('   ⚠️ No attribution keys found');
        return null;
    }
    
    let processedKeys = 0;
    for (const key of attributionKeys) {
        // Skip lookup keys - we want the main pageview records
        if (key.includes('_ip_') || key.includes('_session_') || key.includes('_geo_') || key.includes('_fp_')) {
            continue;
        }
        
        try {
            const pageviewData = await redisRequest('get', key);
            if (!pageviewData) continue;
            
            let pageview;
            try {
                pageview = typeof pageviewData === 'string' ? JSON.parse(pageviewData) : pageviewData;
            } catch (parseError) {
                continue;
            }
            
            // Check if this pageview is in our time window
            const pageviewTime = new Date(pageview.timestamp);
            if (pageviewTime < windowStart || pageviewTime > windowEnd) {
                continue;
            }
            
            // Check if this is an IPv6 pageview (contains colons)
            if (!pageview.ip_address || !pageview.ip_address.includes(':')) {
                continue;
            }
            
            console.log(`   📱 Found IPv6 pageview: ${pageview.ip_address} at ${pageview.timestamp}`);
            
            // Get geographic data for both IPs
            const conversionGeo = await getIPLocation(conversion.ip_address);
            const pageviewGeo = await getIPLocation(pageview.ip_address);
            
            const score = calculateGeoScore(conversionGeo, pageviewGeo);
            
            if (score >= 3) {
                const timeDiff = Math.abs(conversionTime - pageviewTime) / 60000; // minutes
                
                candidates.push({
                    pageview: pageview,
                    score: score,
                    timeDiff: timeDiff,
                    conversionGeo: conversionGeo,
                    pageviewGeo: pageviewGeo
                });
                
                console.log(`   ✅ Candidate found! Score: ${score}, Time diff: ${timeDiff.toFixed(1)} min`);
            }
            
            processedKeys++;
        } catch (error) {
            console.log(`   ⚠️ Error processing key ${key}:`, error.message);
        }
    }
    
    console.log(`   📊 Processed ${processedKeys} pageview keys, found ${candidates.length} candidates`);
    
    if (candidates.length === 0) return null;
    
    // Return best match (highest score, then closest time)
    const bestMatch = candidates.sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        return a.timeDiff - b.timeDiff;
    })[0];
    
    console.log(`   🎯 Best match: Score ${bestMatch.score}, ${bestMatch.timeDiff.toFixed(1)} min gap`);
    return bestMatch;
}

async function getIPLocation(ip) {
    const token = process.env.IPINFO_TOKEN || 'dd31c7ae01d4e4';
    
    try {
        const response = await fetch(`https://ipinfo.io/${ip}?token=${token}`, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        
        if (response.ok) {
            const data = await response.json();
            
            return {
                city: data.city || 'Unknown',
                region: data.region || 'Unknown',
                country: data.country || 'Unknown',
                isp: extractBestISP(data),
                timezone: data.timezone || 'Unknown'
            };
        } else {
            throw new Error(`HTTP ${response.status}`);
        }
    } catch (error) {
        console.log(`   ⚠️ IP lookup failed for ${ip}: ${error.message}`);
        return {
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

function calculateGeoScore(conversionGeo, pageviewGeo) {
    let score = 0;
    
    if (conversionGeo.city === pageviewGeo.city) score += 3;
    if (conversionGeo.region === pageviewGeo.region) score += 2;
    if (conversionGeo.country === pageviewGeo.country) score += 1;
    if (compareISPs(conversionGeo.isp, pageviewGeo.isp)) score += 2;
    
    console.log(`   📊 Geographic score: ${score} (City: ${conversionGeo.city === pageviewGeo.city ? '✓' : '✗'}, Region: ${conversionGeo.region === pageviewGeo.region ? '✓' : '✗'}, Country: ${conversionGeo.country === pageviewGeo.country ? '✓' : '✗'}, ISP: ${compareISPs(conversionGeo.isp, pageviewGeo.isp) ? '✓' : '✗'})`);
    
    return score;
}

function compareISPs(isp1, isp2) {
    if (!isp1 || !isp2 || isp1 === 'Unknown' || isp2 === 'Unknown') return false;
    
    const normalize = str => str.toLowerCase().replace(/[^a-z0-9]/g, '');
    const norm1 = normalize(isp1);
    const norm2 = normalize(isp2);
    
    // Exact match
    if (norm1 === norm2) return true;
    
    // Contains match
    if (norm1.includes(norm2) || norm2.includes(norm1)) return true;
    
    // ASN matching
    const asn1 = isp1.match(/AS(\d+)/);
    const asn2 = isp2.match(/AS(\d+)/);
    if (asn1 && asn2 && asn1[1] === asn2[1]) return true;
    
    return false;
}

async function updateRecoveredAttributions(matches) {
    console.log(`📝 Updating ${matches.length} recovered attributions in Redis`);
    
    for (const match of matches) {
        const conversion = match.conversion;
        const pageview = match.match.pageview;
        
        try {
            // Update conversion record with attribution
            const updatedConversion = {
                ...conversion,
                attribution_found: true,
                landing_page: pageview.landing_page || pageview.url,
                source: pageview.source || 'recovered',
                recovery_method: 'five_phase_geographic',
                recovery_phase: match.phase,
                recovery_confidence: match.confidence,
                recovery_score: match.match.score,
                recovery_timestamp: new Date().toISOString()
            };
            
            // Update the conversion in Redis
            await redisRequest('set', conversion.key, JSON.stringify(updatedConversion));
            
            console.log(`✅ Updated ${conversion.email}: ${pageview.landing_page} (${match.phase})`);
        } catch (error) {
            console.log(`❌ Failed to update ${conversion.email}: ${error.message}`);
        }
    }
}
