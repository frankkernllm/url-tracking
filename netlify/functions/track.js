// File: netlify/functions/track.js
// Enhanced Attribution with IPinfo Geographic Correlation - Solves IPv6/IPv4 dual-stack problem

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
      }
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Method not allowed'
    };
  }

  // API Key validation
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  const isInternalCall = !apiKey; // Webhook calls typically don't have API key
  const isValidExternalCall = apiKey && apiKey === validApiKey;

  if (!isInternalCall && !isValidExternalCall) {
    console.log('🚫 Unauthorized external access attempt');
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  console.log('✅ Track function access authorized');

  // Redis configuration
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };

  // IPinfo Geographic Correlation Service
  class IPinfoService {
    constructor() {
      this.baseUrl = 'https://ipinfo.io';
      this.token = process.env.IPINFO_TOKEN; // Must be set in Netlify environment
      this.cachePrefix = 'geo_cache:';
    }

    async getGeoData(ip) {
      try {
        if (!ip || ip === 'unknown') {
          return this.getFailedLookupData(ip);
        }

        // Check Redis cache first (24-hour TTL)
        const cacheKey = `${this.cachePrefix}${ip.replace(/:/g, '_')}`;
        const cached = await redis(`get/${cacheKey}`);
        
        if (cached.result) {
          const cachedData = JSON.parse(decodeURIComponent(cached.result));
          console.log(`✅ Using cached geo data for ${ip}: ${cachedData.city}, ${cachedData.region} (${cachedData.isp})`);
          return cachedData;
        }

        // Fetch from IPinfo API
        console.log(`🌍 Fetching geo data for ${ip} from IPinfo...`);
        const response = await fetch(`${this.baseUrl}/${ip}?token=${this.token}`, {
          signal: AbortSignal.timeout(2000) // 2 second timeout
        });

        if (!response.ok) {
          throw new Error(`IPinfo API error: ${response.status}`);
        }

        const data = await response.json();
        
        const geoData = {
          ip: data.ip,
          city: data.city || 'Unknown',
          region: data.region || 'Unknown',
          country: data.country || 'Unknown',
          isp: this.extractISP(data),
          coordinates: data.loc || '0,0',
          timezone: data.timezone || 'Unknown',
          lookup_timestamp: new Date().toISOString()
        };

        // Cache in Redis (24 hours = 86400 seconds)
        await redis(`setex/${cacheKey}/86400/${encodeURIComponent(JSON.stringify(geoData))}`);
        console.log(`✅ Cached geo data for ${ip}: ${geoData.city}, ${geoData.region} (${geoData.isp})`);
        
        return geoData;

      } catch (error) {
        console.error(`❌ IPinfo lookup failed for ${ip}:`, error.message);
        return this.getFailedLookupData(ip);
      }
    }

    getFailedLookupData(ip) {
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

    extractISP(data) {
      // Priority hierarchy for ISP identification (critical for dual-stack correlation)
      if (data.company?.name) return data.company.name;
      if (data.asn?.name) return data.asn.name;
      if (data.org) return data.org;
      if (data.carrier?.name) return data.carrier.name;
      return 'Unknown';
    }
  }

  // Enhanced Geographic Attribution Engine - Solves IPv6/IPv4 dual-stack problem
  async function findAttributionWithGeoCorrelation(customerIP, customerEmail) {
    const ipinfoService = new IPinfoService();
    
    console.log('🌍 Starting enhanced attribution search for:', { customerIP, customerEmail });

    // Step 1: Try direct IP lookup first (existing logic - fastest)
    try {
      const lookupResponse = await fetch(
        `https://trackingojoy.netlify.app/.netlify/functions/store-attribution?ip=${customerIP}&timestamp=${new Date().toISOString()}`,
        { 
          method: 'GET',
          headers: { 'X-API-Key': process.env.OJOY_API_KEY }
        }
      );
      
      if (lookupResponse.ok) {
        const lookupResult = await lookupResponse.json();
        if (lookupResult.found) {
          console.log('✅ Direct IP match found (highest confidence)');
          return {
            data: lookupResult.data,
            score: 200,
            method: 'direct_ip_match'
          };
        }
      }
    } catch (error) {
      console.log('⚠️ Direct IP lookup failed:', error.message);
    }

    // Step 2: Geographic correlation for IPv6/IPv4 dual-stack attribution
    console.log('🔍 No direct IP match - starting geographic correlation...');
    
    // Get geographic data for conversion IP
    const conversionGeo = await ipinfoService.getGeoData(customerIP);
    console.log('🌍 Conversion geographic data:', {
      city: conversionGeo.city,
      region: conversionGeo.region,
      isp: conversionGeo.isp,
      country: conversionGeo.country
    });

    if (conversionGeo.city === 'LOOKUP_FAILED') {
      console.log('❌ Cannot perform geographic correlation - geo lookup failed');
      return await fallbackAttributionLookup(customerEmail);
    }

    // Get attribution keys for recent pageviews (focus on IPv6 addresses)
    const windowStart = Date.now() - (2 * 60 * 60 * 1000); // 2 hours ago
    
    try {
      // Get attribution keys using multiple scan patterns
      const scanResults = await Promise.all([
        redis('scan/0/match/attribution_2*/count/500'), // IPv6 starting with 2xxx
        redis('scan/0/match/attribution_*/count/1000')  // All attribution keys
      ]);

      let allKeys = [];
      scanResults.forEach(result => {
        if (result.result && result.result[1]) {
          allKeys = allKeys.concat(result.result[1]);
        }
      });

      // Remove duplicates and filter out lookup keys
      const uniqueKeys = [...new Set(allKeys)].filter(key => 
        key && 
        !key.startsWith('attribution_ip_') && 
        !key.startsWith('attribution_session_')
      );

      console.log(`🔍 Found ${uniqueKeys.length} attribution keys to search for geographic correlation`);

      if (uniqueKeys.length === 0) {
        console.log('❌ No attribution keys found for geographic correlation');
        return await fallbackAttributionLookup(customerEmail);
      }

      let bestMatch = null;
      let bestScore = 0;
      let checkedCount = 0;

      // Check recent attribution records (last 100 for performance)
      const keysToCheck = uniqueKeys.slice(-100);
      
      for (const key of keysToCheck) {
        try {
          const attrResult = await redis(`get/${key}`);
          if (!attrResult.result) continue;

          const attrData = JSON.parse(attrResult.result);
          const attrTimestamp = new Date(attrData.timestamp).getTime();
          
          // Only consider recent attribution (within 2 hours)
          if (attrTimestamp < windowStart) continue;

          // Get geographic data for attribution IP (will use cache if available)
          const attrGeo = await ipinfoService.getGeoData(attrData.ip_address);
          
          // Calculate geographic correlation score
          const geoScore = calculateGeographicScore(conversionGeo, attrGeo);
          
          // Calculate time proximity score (0-30 points)
          const timeDiff = Math.abs(Date.now() - attrTimestamp);
          const timeScore = Math.max(0, 30 - (timeDiff / (2 * 60 * 1000))); // 0-30 based on minutes
          
          // Calculate identity matching score
          let identityScore = 0;
          if (customerEmail && attrData.email === customerEmail) identityScore += 50;
          if (attrData.session_id) identityScore += 10; // Session continuity bonus
          
          const totalScore = geoScore + timeScore + identityScore;
          
          console.log(`🧮 Score for ${attrData.ip_address}:`, {
            geographic: geoScore,
            time: Math.round(timeScore),
            identity: identityScore,
            total: Math.round(totalScore),
            timeDiff: Math.round(timeDiff / 60000) + 'm',
            cities: `${conversionGeo.city} vs ${attrGeo.city}`,
            isps: `${conversionGeo.isp} vs ${attrGeo.isp}`
          });

          if (totalScore > bestScore && totalScore >= 100) { // 100+ point threshold
            bestMatch = attrData;
            bestScore = totalScore;
          }

          checkedCount++;
          
          // Early exit if we find a very high confidence match
          if (totalScore >= 150) {
            console.log(`🎯 High confidence match found early, stopping search`);
            break;
          }

        } catch (parseError) {
          continue; // Skip invalid records
        }
      }

      console.log(`🔍 Checked ${checkedCount} attribution records for geographic correlation`);

      if (bestMatch && bestScore >= 100) {
        let matchMethod = 'geo_correlation';
        if (bestScore >= 150) matchMethod = 'geo_high_confidence';
        else if (bestScore >= 120) matchMethod = 'geo_medium_confidence';
        
        console.log(`✅ Geographic attribution found: ${matchMethod} (score: ${Math.round(bestScore)})`);
        return {
          data: bestMatch,
          score: bestScore,
          method: matchMethod
        };
      }

      console.log(`⚠️ Geographic correlation found matches but scores too low (best: ${Math.round(bestScore)})`);

    } catch (redisError) {
      console.error('❌ Redis operations failed during geographic correlation:', redisError);
    }

    // Step 3: Fallback to existing email-based attribution
    return await fallbackAttributionLookup(customerEmail);
  }

  // Calculate geographic correlation score (key innovation for IPv6/IPv4 matching)
  function calculateGeographicScore(conversionGeo, attrGeo) {
    let score = 0;
    
    // Skip if either lookup failed
    if (conversionGeo.city === 'LOOKUP_FAILED' || attrGeo.city === 'LOOKUP_FAILED') {
      return 0;
    }

    // ISP + Location combinations (primary correlation method for dual-stack)
    if (conversionGeo.isp !== 'Unknown' && attrGeo.isp !== 'Unknown') {
      if (normalizeISP(conversionGeo.isp) === normalizeISP(attrGeo.isp)) {
        if (conversionGeo.city === attrGeo.city) {
          score += 60; // High confidence: same city + ISP (Charlotte, NC + TWC-11426-CAROLINAS)
        } else if (conversionGeo.region === attrGeo.region) {
          score += 40; // Medium confidence: same region + ISP  
        } else if (conversionGeo.country === attrGeo.country) {
          score += 20; // Low confidence: same country + ISP
        }
      }
    }

    // Geographic-only fallbacks (lower confidence)
    if (conversionGeo.city === attrGeo.city && conversionGeo.city !== 'Unknown') {
      score += 20; // Same city bonus
    }
    if (conversionGeo.region === attrGeo.region && conversionGeo.region !== 'Unknown') {
      score += 10; // Same region bonus
    }

    return score;
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

  // Fallback attribution lookup (existing email-based logic)
  async function fallbackAttributionLookup(customerEmail) {
    if (!customerEmail) {
      console.log('❌ No email provided for fallback attribution lookup');
      return null;
    }

    console.log('🔍 Trying fallback email-based attribution lookup...');
    
    try {
      const emailKeys = await redis('keys/attribution_*');
      if (!emailKeys.result || emailKeys.result.length === 0) {
        console.log('❌ No attribution keys found for fallback lookup');
        return null;
      }

      const recentKeys = emailKeys.result.slice(-20); // Check last 20 records
      let bestMatch = null;
      let bestMatchScore = 0;
      
      for (const key of recentKeys) {
        try {
          const attrData = await redis(`get/${key}`);
          if (!attrData.result) continue;

          const parsedData = JSON.parse(attrData.result);
          const timeDiff = Date.now() - new Date(parsedData.timestamp).getTime();
          
          // Only consider attribution within 1 hour
          if (timeDiff < 1 * 60 * 60 * 1000) {
            let matchScore = 0;
            
            if (parsedData.email && parsedData.email === customerEmail) {
              matchScore += 100;
            }
            
            const timeScore = Math.max(0, 10 - (timeDiff / (10 * 60 * 1000)));
            matchScore += timeScore;
            
            if (matchScore > bestMatchScore) {
              bestMatch = parsedData;
              bestMatchScore = matchScore;
            }
          }
        } catch (parseError) {
          continue;
        }
      }
      
      if (bestMatch && bestMatchScore >= 100) {
        console.log(`✅ Fallback attribution found via email match (score: ${bestMatchScore})`);
        return {
          data: bestMatch,
          score: bestMatchScore,
          method: 'email_fallback'
        };
      }

      console.log(`❌ No fallback attribution match found (best score: ${bestMatchScore})`);
      return null;

    } catch (error) {
      console.log('❌ Fallback attribution lookup failed:', error.message);
      return null;
    }
  }

  try {
    const data = JSON.parse(event.body);
    
    console.log('📥 Raw webhook/conversion data:', JSON.stringify(data, null, 2));
    
    const isSpiffyWebhook = data.email && (data.order_total || data.order_id || data.name_first);
    
    let attributionResult = null;
    
    if (isSpiffyWebhook) {
      console.log('🛒 Spiffy webhook detected, starting enhanced attribution lookup...');
      
      // Extract customer IP from webhook payload (not server headers)
      const customerIP = data.checkoutview?.pageviewcheckout?.pageview?.ip || 
                        data.pageview?.ip || 
                        data.ip ||
                        event.headers['x-forwarded-for']?.split(',')[0]?.trim() || 
                        event.headers['x-real-ip'] || 
                        'unknown';
      
      console.log('🔍 Customer IP extracted:', customerIP);
      
      // Enhanced attribution with geographic correlation
      attributionResult = await findAttributionWithGeoCorrelation(customerIP, data.email);
    }
    
    const attributionData = attributionResult?.data || null;
    const attributionMethod = attributionResult?.method || 'none';
    const attributionScore = attributionResult?.score || 0;
    
    if (attributionResult) {
      console.log(`✅ Attribution successful: ${attributionMethod} (score: ${Math.round(attributionScore)})`);
    } else {
      console.log('❌ No attribution found via any method');
    }
    
    // Extract customer data from Spiffy webhook payload
    const customerUserAgent = data.checkoutview?.pageviewcheckout?.pageview?.user_agent || 
                             data.pageview?.user_agent || 
                             data.user_agent ||
                             attributionData?.user_agent || 
                             event.headers['user-agent'] || '';
    
    const customerIPAddress = data.checkoutview?.pageviewcheckout?.pageview?.ip || 
                             data.pageview?.ip || 
                             data.ip ||
                             event.headers['x-forwarded-for'] || '';
    
    // Extract landing page from webhook or attribution
    const landingPage = data.checkoutview?.pageviewcheckout?.pageview?.url ||
                       attributionData?.landing_page ||
                       data.page_url || '';
    
    const trackingData = {
      timestamp: new Date().toISOString(),
      event_type: isSpiffyWebhook ? 'purchase' : 'conversion',
      
      source: attributionData?.source || 
              data.utm_source || data.source || 
              'direct',
      campaign: attributionData?.utm_campaign || 
                data.utm_campaign || data.campaign || 
                'none',
      content: attributionData?.utm_content || 
               data.utm_content || data.content || 
               'none',
      medium: attributionData?.utm_medium || 
              data.utm_medium || 
              'none',
      
      source_type: attributionData?.source_type || 'unknown',
      referrer_url: attributionData?.referrer_url || data.page_url || '',
      landing_page: landingPage,
      
      page_url: data.page_url || landingPage || '',
      conversion_page: data.conversion_page || '',
      
      email: data.email || '',
      name: data.name || data.name_first || '',
      phone: data.phone || data.phone_number || '',
      
      ...(isSpiffyWebhook && {
        order_id: data.order_id,
        order_total: data.order_total,
        currency: data.currency,
        offer_name: data.offer_name,
        payment_gateway: data.payment_gateway,
        subscription_id: data.subscription_id
      }),
      
      // Use customer data from webhook payload, not server headers
      ip_address: customerIPAddress,
      user_agent: customerUserAgent,
      
      ...(attributionData && {
        screen_resolution: attributionData.screen_resolution,
        timezone: attributionData.timezone,
        language: attributionData.language,
        is_returning_visitor: attributionData.is_returning_visitor
      }),
      
      attribution_found: !!attributionData,
      attribution_method: attributionMethod,
      attribution_score: Math.round(attributionScore),
      attribution_source: attributionData ? 'lookup' : 'direct'
    };
    
    console.log('📊 Final tracking data:', JSON.stringify(trackingData, null, 2));
    
    // Store attribution method stats for monitoring
    if (isSpiffyWebhook) {
      try {
        const statsKey = `attribution_stats_${Date.now()}`;
        const stats = {
          timestamp: new Date().toISOString(),
          method: attributionMethod,
          score: Math.round(attributionScore),
          customer_ip: customerIPAddress,
          success: !!attributionData,
          email: data.email
        };
        await redis(`setex/${statsKey}/3600/${encodeURIComponent(JSON.stringify(stats))}`); // 1 hour TTL
      } catch (statsError) {
        console.log('⚠️ Attribution stats storage failed:', statsError.message);
      }
    }
    
    // Store conversion data to Redis
    try {
      if (isSpiffyWebhook) {
        const key = `conversions:${trackingData.timestamp}:${Math.random()}`;
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(trackingData))}`);
        console.log('📊 Conversion stored for analytics');
      } else {
        const key = `pageviews:${trackingData.timestamp}:${Math.random()}`;
        const pageViewData = { ...trackingData, event_type: 'page_view' };
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(pageViewData))}`);
        console.log('📊 Page view stored for analytics');
      }
    } catch (storageError) {
      console.log('⚠️ Redis storage failed:', storageError.message);
    }
    
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ 
        success: true, 
        message: 'Conversion tracked successfully',
        data: trackingData,
        attribution_found: !!attributionData,
        attribution_method: attributionMethod,
        attribution_score: Math.round(attributionScore)
      })
    };
  } catch (error) {
    console.error('❌ Tracking error:', error);
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: error.message })
    };
  }
};

module.exports = { handler };
