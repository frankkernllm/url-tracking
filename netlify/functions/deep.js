// Updated getOrFetchGeoData - Resilient like old version
async function getOrFetchGeoData(ip, geoDataCache, cacheStats) {
    // Try cache first
    let geoData = await getCachedGeoData(ip, geoDataCache, cacheStats);
    if (geoData) return geoData;
    
    // REMOVED: Rate limiting check - let API calls fail naturally
    // Make API call without rate limiting (like old version)
    try {
        const ipinfoToken = process.env.IPINFO_TOKEN;
        if (!ipinfoToken) return getFailedLookupData(ip);
        
        // Shorter timeout for faster failure
        const response = await fetch(`https://ipinfo.io/${ip}?token=${ipinfoToken}`, {
            signal: AbortSignal.timeout(2000) // 2 seconds like old version
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
            return geoData;
        }
    } catch (error) {
        // SILENT FAILURE - just like old version
        // Don't log rate limit errors, just continue
        cacheStats.api_calls++;
    }
    
    // Return failed lookup data and continue processing
    return getFailedLookupData(ip);
}

// Updated findBestTemporalMatch24Hour - No early termination
async function findBestTemporalMatch24Hour(conversion, candidatePageviews, conversionGeoData, geoDataCache, cacheStats) {
    for (let i = 0; i < candidatePageviews.length; i++) {
        const pageview = candidatePageviews[i];
        const timeDiff = Math.abs(new Date(conversion.timestamp) - new Date(pageview.timestamp)) / 1000 / 60;
        
        // Get pageview geographic data (using cache)
        const pageviewGeoData = await getOrFetchGeoData(pageview.ip_address, geoDataCache, cacheStats);
        
        // Compare geographic data with enhanced scoring
        const geoMatch = compareGeographicDataEnhanced(conversionGeoData, pageviewGeoData);
        
        if (geoMatch.isMatch) {
            console.log(`      ✅ 24-hour geographic correlation successful: ${geoMatch.confidence} (score: ${Math.round(geoMatch.score)})`);
            
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
        
        // REMOVED: Early termination on API limits
        // Continue processing regardless of API call results
        
        // Progress logging for large datasets (less frequent to reduce noise)
        if ((i + 1) % 100 === 0) {
            console.log(`      📊 Checked ${i + 1}/${candidatePageviews.length} pageviews`);
        }
    }
    
    return null;
}

// Enhanced geographic comparison - handles LOOKUP_FAILED gracefully
function compareGeographicDataEnhanced(conversionGeo, pageviewGeo) {
    // IMPROVED: Handle lookup failures more gracefully
    if (conversionGeo.city === 'LOOKUP_FAILED' || pageviewGeo.city === 'LOOKUP_FAILED') {
        return { isMatch: false, confidence: 'LOOKUP_FAILED', score: 0 };
    }

    const cityMatch = conversionGeo.city === pageviewGeo.city;
    const regionMatch = conversionGeo.region === pageviewGeo.region;
    const countryMatch = conversionGeo.country === pageviewGeo.country;
    const ispMatch = compareISPs(conversionGeo.isp, pageviewGeo.isp);

    let score = 0;
    
    // ISP + Location combinations (primary correlation method for dual-stack)
    if (conversionGeo.isp !== 'Unknown' && pageviewGeo.isp !== 'Unknown' && 
        conversionGeo.isp !== 'LOOKUP_FAILED' && pageviewGeo.isp !== 'LOOKUP_FAILED') {
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
    if (cityMatch && conversionGeo.city !== 'Unknown' && conversionGeo.city !== 'LOOKUP_FAILED') {
        score += 20; // Same city bonus
    }
    if (regionMatch && conversionGeo.region !== 'Unknown' && conversionGeo.region !== 'LOOKUP_FAILED') {
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

// Updated getCachedGeoData - More resilient error handling
async function getCachedGeoData(ip, geoDataCache, cacheStats) {
    if (!ip || ip === 'unknown') return null;
    
    // Check in-memory cache first (for current batch run)
    if (geoDataCache.has(ip)) {
        cacheStats.hits++;
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
                    return attrData.geographic_data;
                }
            }
        }
        
        cacheStats.misses++;
        return null;
        
    } catch (error) {
        // SILENT FAILURE - don't log cache errors
        cacheStats.misses++;
        return null;
    }
}

// Updated performGeographicCorrelation24Hour - More resilient
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

        // CONTINUE EVEN IF LOOKUP FAILED - might still find matches with cached pageview data
        
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
    
    console.log('      📊 24-hour geographic correlation complete');
    console.log(`      📊 Cache performance: ${cacheStats.hits} hits, ${cacheStats.misses} misses, ${cacheStats.api_calls} API calls`);
    return null;
}

// SIMPLIFIED: Less verbose logging for better performance
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
