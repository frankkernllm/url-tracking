// File: netlify/functions/track.js
// Enhanced Attribution with 6-Tier Priority System + IPv6/IPv4 Dual-Stack + Device Fingerprinting
// Maintains full backward compatibility with existing geographic correlation

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

  // IPv6-safe key encoding (matching store-attribution.js)
  function encodeIPForKey(ip) {
    return ip.replace(/:/g, '_');
  }

  // Hash function for privacy-safe parameter values (matching landing page script)
  function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36); // Convert to base36 for shorter string
  }

  // ✅ ENHANCED: Multi-priority attribution function with 7-tier system
  async function findEnhancedAttribution(webhookData) {
    console.log('🔍 Starting enhanced 7-tier attribution search...', {
      SSID: webhookData.SSID || 'none',
      PIP: webhookData.PIP || 'none',
      CIP: webhookData.CIP || 'none', 
      IP: webhookData.IP || 'none',
      dsig: webhookData.dsig?.substring(0, 10) + '...' || 'none',
      SVV: webhookData.SVV || 'none',
      gsig: webhookData.gsig || 'none'
    });
    
    // Priority 1: Session ID Match (300 points) - HIGHEST PRIORITY
    if (webhookData.SSID) {
      console.log('🔍 Priority 1: Trying SSID match:', webhookData.SSID);
      const sessionKey = `attribution_session_${webhookData.SSID}`;
      const sessionResult = await redis(`get/${sessionKey}`);
      
      if (sessionResult.result) {
        const mainKey = sessionResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 1: SSID match found - highest confidence');
          return {
            method: 'ssid_direct_match',
            score: 300,
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 1: SSID lookup failed');
    }
    
    // Priority 2: Primary IP Match (280 points) - IPv6 Original or Explicit Primary IP
    if (webhookData.PIP) {
      console.log('🔍 Priority 2: Trying Primary IP match:', webhookData.PIP);
      const pipKey = `attribution_ip_${encodeIPForKey(webhookData.PIP)}`;
      const pipResult = await redis(`get/${pipKey}`);
      
      if (pipResult.result) {
        const mainKey = pipResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 2: Primary IP match found');
          return {
            method: 'primary_ip_match',
            score: 280,
            matched_ip: 'primary',
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 2: Primary IP lookup failed');
    }
    
    // Priority 3: Conversion IP Match (260 points) - Top-level checkout IP
    if (webhookData.CIP && webhookData.CIP !== webhookData.PIP) {
      console.log('🔍 Priority 3: Trying Conversion IP match:', webhookData.CIP);
      const cipKey = `attribution_ip_${encodeIPForKey(webhookData.CIP)}`;
      const cipResult = await redis(`get/${cipKey}`);
      
      if (cipResult.result) {
        const mainKey = cipResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 3: Conversion IP match found');
          return {
            method: 'conversion_ip_match',
            score: 260,
            matched_ip: 'conversion',
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 3: Conversion IP lookup failed');
    }
    
    // Priority 4: Pageview IP Match (240 points) - Nested pageview IP (backward compatibility)
    if (webhookData.IP && webhookData.IP !== webhookData.CIP && webhookData.IP !== webhookData.PIP) {
      console.log('🔍 Priority 4: Trying Pageview IP match:', webhookData.IP);
      const ipKey = `attribution_ip_${encodeIPForKey(webhookData.IP)}`;
      const ipResult = await redis(`get/${ipKey}`);
      
      if (ipResult.result) {
        const mainKey = ipResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 4: Pageview IP match found');
          return {
            method: 'pageview_ip_match',
            score: 240,
            matched_ip: 'pageview',
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 4: Pageview IP lookup failed');
    }
    
    // Priority 5: Device Signature Match (220 points) - Cross-device attribution
    if (webhookData.dsig) {
      console.log('🔍 Priority 5: Trying device signature match:', webhookData.dsig);
      const fpKey = `attribution_fp_${webhookData.dsig}`;
      const fpResult = await redis(`get/${fpKey}`);
      
      if (fpResult.result) {
        const mainKey = fpResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 5: Device signature match found - cross-device attribution');
          return {
            method: 'device_signature_match',
            score: 220,
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 5: Device signature lookup failed');
    }
    
    // Priority 6: Screen Hash Match (200 points) - Privacy-safe device correlation
    if (webhookData.SVV) {
      console.log('🔍 Priority 6: Trying screen hash match:', webhookData.SVV);
      const screenKey = `attribution_screen_${webhookData.SVV}`;
      const screenResult = await redis(`get/${screenKey}`);
      
      if (screenResult.result) {
        const mainKey = screenResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 6: Screen hash match found - privacy-safe device correlation');
          return {
            method: 'screen_hash_match',
            score: 200,
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 6: Screen hash lookup failed');
    }
    
    // Priority 7: WebGL Signature Match (180 points) - Additional device validation
    if (webhookData.gsig) {
      console.log('🔍 Priority 7: Trying WebGL signature match:', webhookData.gsig);
      const webglKey = `attribution_webgl_${webhookData.gsig}`;
      const webglResult = await redis(`get/${webglKey}`);
      
      if (webglResult.result) {
        const mainKey = webglResult.result;
        const attributionResult = await redis(`get/${mainKey}`);
        if (attributionResult.result) {
          console.log('✅ Priority 7: WebGL signature match found');
          return {
            method: 'webgl_signature_match',
            score: 180,
            ...JSON.parse(attributionResult.result)
          };
        }
      }
      console.log('⚠️ Priority 7: WebGL signature lookup failed');
    }
    
    // Priority 8: Geographic Correlation (60-100 points) - EXISTING LOGIC MAINTAINED
    console.log('🔍 Priority 8: Falling back to geographic correlation');
    const testIPs = [webhookData.PIP, webhookData.CIP, webhookData.IP].filter(Boolean);
    const geoResult = await tryGeographicCorrelation(testIPs);
    if (geoResult) {
      console.log('✅ Priority 8: Geographic correlation successful');
      return geoResult;
    }
    
    console.log('❌ All 8 attribution priorities failed');
    return null;
  } '

  // EXISTING: Geographic correlation function (maintained for backward compatibility)
  async function tryGeographicCorrelation(customerIPs) {
    if (!customerIPs || customerIPs.length === 0) {
      console.log('❌ No IPs provided for geographic correlation');
      return null;
    }

    console.log('🌍 Starting geographic correlation for IPs:', customerIPs);
    
    const ipinfoService = new IPinfoService();
    
    // Test each customer IP for geographic correlation
    for (const customerIP of customerIPs) {
      console.log(`🌍 Testing geographic correlation for: ${customerIP}`);
      
      // Get geographic data for conversion IP (check cache first)
      const conversionGeo = await getCachedGeoData(customerIP) || 
                           await ipinfoService.getGeoData(customerIP);
      
      console.log('🌍 Conversion geographic data:', {
        city: conversionGeo.city,
        region: conversionGeo.region,
        isp: conversionGeo.isp,
        country: conversionGeo.country
      });

      if (conversionGeo.city === 'LOOKUP_FAILED') {
        console.log('❌ Geographic lookup failed, trying next IP');
        continue;
      }

      // Search attribution records with existing timing logic
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
          !key.startsWith('attribution_session_') &&
          !key.startsWith('attribution_fp_') &&
          !key.startsWith('attribution_webgl_') &&
          !key.startsWith('attribution_screen_')
        );

        console.log(`🔍 Found ${uniqueKeys.length} attribution keys for geographic correlation`);

        if (uniqueKeys.length === 0) {
          continue;
        }

        // Process keys and sort by timestamp
        let validRecords = [];

        for (const key of uniqueKeys) {
          try {
            const attrResult = await redis(`get/${key}`);
            if (!attrResult.result) continue;

            const attrData = JSON.parse(attrResult.result);
            const attrTimestamp = new Date(attrData.timestamp).getTime();
            
            validRecords.push({
              key,
              data: attrData,
              timestamp: attrTimestamp
            });
            
            if (validRecords.length >= 200) break;
            
          } catch (parseError) {
            continue;
          }
        }

        // Sort and filter by time window
        const recentRecords = validRecords
          .filter(record => record.timestamp >= windowStart)
          .sort((a, b) => b.timestamp - a.timestamp)
          .slice(0, 50);

        console.log(`🔍 Found ${recentRecords.length} recent records for geographic correlation`);

        if (recentRecords.length === 0) {
          continue;
        }

        let bestMatch = null;
        let bestScore = 0;

        for (const record of recentRecords) {
          const attrData = record.data;
          const attrTimestamp = record.timestamp;
          
          // Get geographic data for attribution IP
          const attrGeo = await getCachedGeoData(attrData.ip_address) || 
                         await ipinfoService.getGeoData(attrData.ip_address);
          
          // Calculate geographic correlation score
          const geoScore = calculateGeographicScore(conversionGeo, attrGeo);
          
          // Calculate time proximity score
          const timeDiff = Math.abs(Date.now() - attrTimestamp);
          const timeScore = Math.max(0, 30 - (timeDiff / (2 * 60 * 1000)));
          
          const totalScore = geoScore + timeScore;
          
          console.log(`🧮 Geographic score for ${attrData.ip_address}:`, {
            geographic: geoScore,
            time: Math.round(timeScore),
            total: Math.round(totalScore),
            cities: `${conversionGeo.city} vs ${attrGeo.city}`,
            isps: `${conversionGeo.isp} vs ${attrGeo.isp}`
          });

          if (totalScore > bestScore && totalScore >= 60) {
            bestMatch = attrData;
            bestScore = totalScore;
          }

          if (totalScore >= 90) {
            console.log('🎯 High confidence geographic match found early');
            break;
          }
        }

        if (bestMatch && bestScore >= 60) {
          let matchMethod = 'geo_correlation';
          if (bestScore >= 90) matchMethod = 'geo_high_confidence';
          else if (bestScore >= 75) matchMethod = 'geo_medium_confidence';
          
          console.log(`✅ Geographic correlation successful: ${matchMethod} (score: ${Math.round(bestScore)})`);
          return {
            data: bestMatch,
            score: bestScore,
            method: matchMethod
          };
        }

      } catch (error) {
        console.log('❌ Geographic correlation error:', error.message);
        continue;
      }
    }

    console.log('❌ Geographic correlation failed for all IPs');
    return null;
  }

  // NEW: Get cached geographic data from attribution records (major optimization)
  async function getCachedGeoData(ip) {
    try {
      // Check if we already have geo data for this IP from pageview attribution
      const encodedIP = ip.replace(/:/g, '_');
      const ipKey = `attribution_ip_${encodedIP}`;
      const attrKeyResult = await redis(`get/${ipKey}`);
      
      if (attrKeyResult.result) {
        const attrResult = await redis(`get/${attrKeyResult.result}`);
        if (attrResult.result) {
          const attrData = JSON.parse(attrResult.result);
          if (attrData.geographic_data) {
            return attrData.geographic_data;
          }
        }
      }
      
      // Also check geo cache
      const geoKeys = await redis(`keys/attribution_geo_*`);
      if (geoKeys.result && geoKeys.result.length > 0) {
        for (const geoKey of geoKeys.result.slice(-20)) {
          try {
            const mainKeyResult = await redis(`get/${geoKey}`);
            if (mainKeyResult.result) {
              const mainKey = mainKeyResult.result;
              const attrResult = await redis(`get/${mainKey}`);
              if (attrResult.result) {
                const attrData = JSON.parse(attrResult.result);
                if (attrData.ip_address === ip && attrData.geographic_data) {
                  return attrData.geographic_data;
                }
              }
            }
          } catch (geoError) {
            continue;
          }
        }
      }
      
      return null;
    } catch (error) {
      return null;
    }
  }

  // IPinfo Geographic Correlation Service (EXISTING - MAINTAINED)
  class IPinfoService {
    constructor() {
      this.baseUrl = 'https://ipinfo.io';
      this.token = process.env.IPINFO_TOKEN;
      this.cachePrefix = 'geo_cache:';
    }

    async getGeoData(ip) {
      try {
        if (!ip || ip === 'unknown') {
          return this.getFailedLookupData(ip);
        }

        const cacheKey = `${this.cachePrefix}${ip.replace(/:/g, '_')}`;
        const cached = await redis(`get/${cacheKey}`);
        
        if (cached.result) {
          const cachedData = JSON.parse(decodeURIComponent(cached.result));
          console.log(`✅ Using cached geo data for ${ip}: ${cachedData.city}, ${cachedData.region} (${cachedData.isp})`);
          return cachedData;
        }

        console.log(`🌍 Fetching geo data for ${ip} from IPinfo...`);
        const response = await fetch(`${this.baseUrl}/${ip}?token=${this.token}`, {
          signal: AbortSignal.timeout(2000)
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
      if (data.company?.name) return data.company.name;
      if (data.asn?.name) return data.asn.name;
      if (data.org) return data.org;
      if (data.carrier?.name) return data.carrier.name;
      return 'Unknown';
    }
  }

  // Calculate geographic correlation score (EXISTING - MAINTAINED)
  function calculateGeographicScore(conversionGeo, attrGeo) {
    let score = 0;
    
    if (conversionGeo.city === 'LOOKUP_FAILED' || attrGeo.city === 'LOOKUP_FAILED') {
      return 0;
    }

    if (conversionGeo.isp !== 'Unknown' && attrGeo.isp !== 'Unknown') {
      if (normalizeISP(conversionGeo.isp) === normalizeISP(attrGeo.isp)) {
        if (conversionGeo.city === attrGeo.city) {
          score += 60;
        } else if (conversionGeo.region === attrGeo.region) {
          score += 40;
        } else if (conversionGeo.country === attrGeo.country) {
          score += 20;
        }
      }
    }

    if (conversionGeo.city === attrGeo.city && conversionGeo.city !== 'Unknown') {
      score += 20;
    }
    if (conversionGeo.region === attrGeo.region && conversionGeo.region !== 'Unknown') {
      score += 10;
    }

    return score;
  }

  // Normalize ISP names for better matching (EXISTING - MAINTAINED)
  function normalizeISP(isp) {
    if (!isp || isp === 'Unknown') return '';
    
    const normalized = isp.toLowerCase().replace(/[^a-z0-9]/g, '');
    
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

  // Fallback attribution lookup (EXISTING - MAINTAINED)
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

      const recentKeys = emailKeys.result.slice(-20);
      let bestMatch = null;
      let bestMatchScore = 0;
      
      for (const key of recentKeys) {
        try {
          const attrData = await redis(`get/${key}`);
          if (!attrData.result) continue;

          const parsedData = JSON.parse(attrData.result);
          const timeDiff = Date.now() - new Date(parsedData.timestamp).getTime();
          
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
    
    // ✅ ENHANCED: Extract all attribution data from webhook (FIXED to match actual Spiffy fields)
    const extractedData = {
      // Basic conversion data
      email: data.email || data.customer?.email,
      order_total: data.order_total || 0,
      order_id: data.order_id,
      offer_name: data.offer_name,
      
      // ✅ FIXED: IP extraction mapping to actual Spiffy webhook structure
      PIP: data.custom_ipv6 || data.custom_ipv4,                                   // Primary IP (explicit custom fields)
      CIP: data.ip,                                                                // Conversion IP (top level)
      IP: data.checkoutview?.pageviewcheckout?.pageview?.ip,                       // Pageview IP (nested - backward compatibility)
      
      // ✅ FIXED: Custom attribution parameters matching actual Spiffy webhook field names
      SSID: data.ssid,                                                             // Session ID (lowercase)
      dsig: data.dsig,                                                             // Device signature (canvas slice)
      SVV: data.svvv,                                                              // Screen value (hashed - webhook sends svvv)
      gsig: data.gsig,                                                             // GPU signature (hashed)
      
      // UTM parameters from multiple locations (EXISTING - MAINTAINED)
      utm_source: data.utm_source ||
                 data.checkoutview?.utm_source ||
                 data.checkoutview?.pageviewcheckout?.pageview?.utm_source ||
                 data.ref_utm_source,
                 
      utm_campaign: data.utm_campaign ||
                   data.checkoutview?.utm_campaign ||
                   data.checkoutview?.pageviewcheckout?.pageview?.utm_campaign ||
                   data.ref_utm_campaign,
                   
      utm_medium: data.utm_medium ||
                 data.checkoutview?.utm_medium ||
                 data.checkoutview?.pageviewcheckout?.pageview?.utm_medium ||
                 data.ref_utm_medium,
                 
      utm_content: data.utm_content ||
                  data.checkoutview?.utm_content ||
                  data.checkoutview?.pageviewcheckout?.pageview?.utm_content ||
                  data.ref_utm_content,
                  
      utm_term: data.utm_term ||
               data.checkoutview?.utm_term ||
               data.checkoutview?.pageviewcheckout?.pageview?.utm_term ||
               data.ref_utm_term,
      
      // Enhanced device data
      user_agent: data.checkoutview?.pageviewcheckout?.pageview?.user_agent || 
                 data.user_agent,
      
      // Timing
      timestamp: new Date().toISOString(),
      webhook_timestamp: data.created_at,
      pageview_timestamp: data.checkoutview?.pageviewcheckout?.pageview?.created_at
    };
    
    console.log('📊 Enhanced webhook data extracted:', {
      email: extractedData.email,
      PIP: extractedData.PIP,
      CIP: extractedData.CIP,
      IP: extractedData.IP,
      SSID: extractedData.SSID,
      dsig: extractedData.dsig?.substring(0, 10) + '...' || 'null',
      SVV: extractedData.SVV,
      gsig: extractedData.gsig,
      utm_source: extractedData.utm_source,
      utm_campaign: extractedData.utm_campaign
    });
    
    // Detect dual IP scenario (IPv6 to IPv4 conversion)
    const isDualIP = extractedData.PIP && extractedData.CIP && extractedData.PIP !== extractedData.CIP;
    if (isDualIP) {
      console.log('🔄 Dual IP detected - IPv6 to IPv4 conversion scenario:', {
        PIP: extractedData.PIP,
        CIP: extractedData.CIP
      });
    } else if (extractedData.PIP && extractedData.CIP && extractedData.PIP === extractedData.CIP) {
      console.log('📍 Single IP scenario - same address throughout:', {
        IP: extractedData.PIP
      });
    }
    
    const isSpiffyWebhook = extractedData.email && (extractedData.order_total || extractedData.order_id);
    
    let attributionResult = null;
    
    if (isSpiffyWebhook) {
      console.log('🛒 Spiffy webhook detected, starting enhanced 8-tier attribution lookup...');
      
      // ✅ ENHANCED: Use new 8-tier attribution system with proper IP handling
      attributionResult = await findEnhancedAttribution(extractedData);
      
      // ✅ FALLBACK: If enhanced attribution fails, try existing geographic correlation
      if (!attributionResult) {
        console.log('🔄 Enhanced attribution failed, trying geographic correlation...');
        const testIPs = [extractedData.PIP, extractedData.CIP, extractedData.IP].filter(Boolean);
        attributionResult = await tryGeographicCorrelation(testIPs);
      }
      
      // ✅ FINAL FALLBACK: Email-based lookup
      if (!attributionResult) {
        console.log('🔄 Geographic correlation failed, trying email fallback...');
        attributionResult = await fallbackAttributionLookup(extractedData.email);
      }
    }
    
    const attributionData = attributionResult?.data || null;
    const attributionMethod = attributionResult?.method || 'none';
    const attributionScore = attributionResult?.score || 0;
    
    if (attributionResult) {
      console.log(`✅ Attribution successful: ${attributionMethod} (score: ${Math.round(attributionScore)})`);
    } else {
      console.log('❌ No attribution found via any method');
    }
    
    // EXISTING: Build tracking data (MAINTAINED)
    const trackingData = {
      timestamp: new Date().toISOString(),
      event_type: isSpiffyWebhook ? 'purchase' : 'conversion',
      
      source: attributionData?.source || 
              extractedData.utm_source || 
              'direct',
      campaign: attributionData?.utm_campaign || 
                extractedData.utm_campaign || 
                'none',
      content: attributionData?.utm_content || 
               extractedData.utm_content || 
               'none',
      medium: attributionData?.utm_medium || 
              extractedData.utm_medium || 
              'none',
      
      source_type: attributionData?.source_type || 'unknown',
      referrer_url: attributionData?.referrer_url || '',
      landing_page: attributionData?.landing_page || 'unknown',
      
      email: extractedData.email || '',
      name: data.name || data.name_first || '',
      phone: data.phone || data.phone_number || '',
      
      ...(isSpiffyWebhook && {
        order_id: extractedData.order_id,
        order_total: extractedData.order_total,
        currency: data.currency,
        offer_name: extractedData.offer_name,
        payment_gateway: data.payment_gateway,
        subscription_id: data.subscription_id
      }),
      
      ip_address: extractedData.CIP || extractedData.PIP || extractedData.IP,
      user_agent: extractedData.user_agent || '',
      
      ...(attributionData && {
        screen_resolution: attributionData.screen_resolution,
        timezone: attributionData.timezone,
        language: attributionData.language,
        is_returning_visitor: attributionData.is_returning_visitor
      }),
      
      attribution_found: !!attributionData,
      attribution_method: attributionMethod,
      attribution_score: Math.round(attributionScore),
      attribution_source: attributionData ? 'lookup' : 'direct',
      
      // ✅ NEW: Enhanced attribution metadata with proper IP tracking
      dual_ip_scenario: isDualIP,
      primary_ip: extractedData.PIP,
      conversion_ip: extractedData.CIP,
      pageview_ip: extractedData.IP,
      session_id_found: !!extractedData.SSID,
      device_signature_found: !!extractedData.dsig,
      screen_hash_found: !!extractedData.SVV,
      webgl_signature_found: !!extractedData.gsig
    };
    
    console.log('📊 Final enhanced tracking data:', JSON.stringify(trackingData, null, 2));
    
    // EXISTING: Store attribution stats and conversion data (MAINTAINED)
    if (isSpiffyWebhook) {
      try {
        const statsKey = `attribution_stats_${Date.now()}`;
        const stats = {
          timestamp: new Date().toISOString(),
          method: attributionMethod,
          score: Math.round(attributionScore),
          customer_ip: trackingData.ip_address,
          success: !!attributionData,
          email: extractedData.email,
          dual_ip: isDualIP,
          enhanced_attribution: true
        };
        await redis(`setex/${statsKey}/3600/${encodeURIComponent(JSON.stringify(stats))}`);
      } catch (statsError) {
        console.log('⚠️ Attribution stats storage failed:', statsError.message);
      }
    }
    
    // EXISTING: Store conversion/pageview data (MAINTAINED)
    try {
      if (isSpiffyWebhook) {
        const key = `conversions:${trackingData.timestamp}:${Math.random()}`;
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(trackingData))}`);
        console.log('📊 Enhanced conversion stored for analytics');
      } else {
        const key = `pageviews:${trackingData.timestamp}:${Math.random()}`;
        const pageViewData = { ...trackingData, event_type: 'page_view' };
        await redis(`set/${key}/${encodeURIComponent(JSON.stringify(pageViewData))}`);
        console.log('📊 Enhanced page view stored for analytics');
      }
    } catch (storageError) {
      console.log('⚠️ Redis storage failed:', storageError.message);
    }
    
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ 
        success: true, 
        message: 'Enhanced conversion tracked successfully',
        data: trackingData,
        attribution_found: !!attributionData,
        attribution_method: attributionMethod,
        attribution_score: Math.round(attributionScore),
        dual_ip_detected: isDualIP,
        enhanced_attribution: true
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced tracking error:', error);
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: error.message })
    };
  }
};

module.exports = { handler };
