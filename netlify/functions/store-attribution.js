// File: netlify/functions/store-attribution.js
// ENHANCED: IPv6-safe + Device Fingerprinting + Geographic Keys + Direct Lookup Keys (Phase 4)

const handler = async (event, context) => {
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
      }
    };
  }

  // Security Check
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  const validApiKey = process.env.OJOY_API_KEY;

  if (!apiKey || apiKey !== validApiKey) {
    return {
      statusCode: 401,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

  const redis = async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };

  // Extract real visitor IP from request headers
  function getVisitorIP(event) {
    const forwarded = event.headers['x-forwarded-for'];
    const realIP = event.headers['x-real-ip'];
    const cfIP = event.headers['cf-connecting-ip'];
    const clientIP = event.headers['x-client-ip'];
    
    if (forwarded) {
      return forwarded.split(',')[0].trim();
    }
    if (cfIP) return cfIP;
    if (realIP) return realIP;
    if (clientIP) return clientIP;
    
    return 'unknown';
  }

  // IPv6-safe key encoding
  function encodeIPForKey(ip) {
    // Replace colons with underscores for IPv6 addresses
    return ip.replace(/:/g, '_');
  }

  // PHASE 3: Clean string for Redis key usage
  function cleanForRedisKey(str) {
    if (!str || str === 'Unknown' || str === 'unknown') return '';
    return str.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
  }

  // NEW: Hash function for privacy-safe parameter values (matching landing page script)
  function hashString(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return Math.abs(hash).toString(36); // Convert to base36 for shorter string
  }

  if (event.httpMethod === 'POST') {
    try {
      const attributionData = JSON.parse(event.body);
      
      const visitorIP = getVisitorIP(event);
      attributionData.ip_address = visitorIP;
      
      console.log('📊 Storing enhanced attribution data:', {
        session_id: attributionData.session_id,
        ip_address: visitorIP,
        source: attributionData.source,
        landing_page: attributionData.landing_page,
        canvas_fp: attributionData.canvas_fingerprint?.substring(0, 10) + '...',
        webgl_fp: attributionData.webgl_fingerprint?.substring(0, 10) + '...',
        cpu_cores: attributionData.cpu_cores,
        memory_gb: attributionData.memory_gb
      });
      
      const timestamp = Date.now();
      const encodedIP = encodeIPForKey(visitorIP);
      
      // Main attribution key (IPv6-safe)
      const baseKey = `attribution_${encodedIP}_${timestamp}`;
      
      console.log('🔑 Creating base key:', baseKey);
      
      // Store the full attribution data
      await redis(`set/${baseKey}/${encodeURIComponent(JSON.stringify(attributionData))}`);
      console.log('✅ Stored attribution with key:', baseKey);
      
      // Create basic lookup keys with 24-hour expiration (86400 seconds)
      const ipKey = `attribution_ip_${encodedIP}`;
      const sessionKey = `attribution_session_${attributionData.session_id}`;
      
      await redis(`setex/${ipKey}/86400/${baseKey}`);
      await redis(`setex/${sessionKey}/86400/${baseKey}`);
      
      console.log('✅ Created basic lookup keys:', { ipKey, sessionKey });
      
      let additionalKeysCreated = 0;

      // ✅ ENHANCED: Device fingerprint storage with DUAL key pattern for cross-device attribution
      try {
        const deviceFingerprint = attributionData.canvas_fingerprint || '';
        if (deviceFingerprint && deviceFingerprint !== 'unavailable' && deviceFingerprint.length > 10) {
          // 1. EXISTING: Timestamped key for analytics
          const fpHash = deviceFingerprint.slice(-20);
          const fpKey = `attribution_fp_${fpHash}_${timestamp}`;
          await redis(`setex/${fpKey}/86400/${baseKey}`);
          console.log('✅ Created timestamped fingerprint key:', fpKey);
          additionalKeysCreated++;
          
          // 2. NEW: Direct lookup key (no timestamp) for track.js priority matching
          const directFpKey = `attribution_fp_${fpHash}`;
          await redis(`setex/${directFpKey}/86400/${baseKey}`);
          console.log('✅ Created direct fingerprint lookup key:', directFpKey);
          additionalKeysCreated++;
        }

        // ✅ ENHANCED: WebGL fingerprint with dual pattern
        const webglFingerprint = attributionData.webgl_fingerprint || '';
        if (webglFingerprint && webglFingerprint !== 'unavailable' && webglFingerprint !== 'blocked' && webglFingerprint.length > 10) {
          const webglHash = cleanForRedisKey(webglFingerprint.substring(0, 30));
          if (webglHash.length > 5) {
            // 1. EXISTING: Timestamped key
            const webglKey = `attribution_webgl_${webglHash}_${timestamp}`;
            await redis(`setex/${webglKey}/86400/${baseKey}`);
            console.log('✅ Created timestamped WebGL key:', webglKey);
            additionalKeysCreated++;
            
            // 2. NEW: Direct lookup key for hashed WebGL signature (gsig parameter)
            const hashedWebGL = hashString(webglFingerprint);
            const directWebglKey = `attribution_webgl_${hashedWebGL}`;
            await redis(`setex/${directWebglKey}/86400/${baseKey}`);
            console.log('✅ Created direct WebGL lookup key:', directWebglKey);
            additionalKeysCreated++;
          }
        }

        // ✅ NEW: Screen resolution hash key (SVV parameter from landing page)
        const screenResolution = attributionData.screen_resolution || '';
        if (screenResolution && screenResolution !== 'unknown') {
          const screenHash = hashString(screenResolution);
          const screenKey = `attribution_screen_${screenHash}`;
          await redis(`setex/${screenKey}/86400/${baseKey}`);
          console.log('✅ Created screen hash key:', screenKey);
          additionalKeysCreated++;
        }

        // ✅ NEW: Enhanced hardware fingerprinting keys
        if (attributionData.cpu_cores && attributionData.cpu_cores !== 'unknown') {
          const hardwareKey = `attribution_hw_${attributionData.cpu_cores}_${attributionData.memory_gb || 'unknown'}`;
          await redis(`setex/${hardwareKey}/86400/${baseKey}`);
          console.log('✅ Created hardware fingerprint key:', hardwareKey);
          additionalKeysCreated++;
        }

      } catch (fpError) {
        console.log('⚠️ Enhanced fingerprint key creation failed:', fpError.message);
      }

      // PHASE 3: Geographic keys for faster correlation (IPv6/IPv4 dual-stack solution)
      if (visitorIP !== 'unknown') {
        try {
          console.log('🌍 Creating geographic correlation keys...');
          
          // Fetch geographic data from IPinfo API
          const ipinfoToken = process.env.IPINFO_TOKEN;
          if (ipinfoToken) {
            const geoResponse = await fetch(`https://ipinfo.io/${visitorIP}?token=${ipinfoToken}`, {
              signal: AbortSignal.timeout(3000) // 3 second timeout
            });
            
            if (geoResponse.ok) {
              const geoData = await geoResponse.json();
              
              // Extract ISP/organization info
              const isp = geoData.company?.name || geoData.asn?.name || geoData.org || 'unknown';
              const city = geoData.city || 'unknown';
              const region = geoData.region || 'unknown';
              
              console.log('🌍 Geographic data obtained:', { 
                city, 
                region, 
                isp: isp.substring(0, 30) 
              });
              
              // Create geographic correlation keys for IPv6/IPv4 matching
              if (city !== 'unknown' && isp !== 'unknown') {
                const cleanCity = cleanForRedisKey(city);
                const cleanISP = cleanForRedisKey(isp.substring(0, 20)); // Limit ISP length
                
                if (cleanCity.length > 2 && cleanISP.length > 2) {
                  const geoKey = `attribution_geo_${cleanCity}_${cleanISP}_${timestamp}`;
                  await redis(`setex/${geoKey}/86400/${baseKey}`);
                  console.log('✅ Created geographic key:', geoKey);
                  additionalKeysCreated++;
                }
                
                // Additional regional key for broader matching
                if (region !== 'unknown') {
                  const cleanRegion = cleanForRedisKey(region);
                  if (cleanRegion.length > 2) {
                    const regionKey = `attribution_region_${cleanRegion}_${cleanISP}_${timestamp}`;
                    await redis(`setex/${regionKey}/86400/${baseKey}`);
                    console.log('✅ Created regional key:', regionKey);
                    additionalKeysCreated++;
                  }
                }
              }
              
              // Cache the geographic data for the track.js function to use
              const cacheKey = `geo_cache:${encodedIP}`;
              const cacheData = {
                ip: geoData.ip,
                city: geoData.city || 'Unknown',
                region: geoData.region || 'Unknown',
                country: geoData.country || 'Unknown',
                isp: isp,
                coordinates: geoData.loc || '0,0',
                timezone: geoData.timezone || 'Unknown',
                lookup_timestamp: new Date().toISOString()
              };
              
              await redis(`setex/${cacheKey}/86400/${encodeURIComponent(JSON.stringify(cacheData))}`);
              console.log('✅ Cached geographic data for track.js usage');
              additionalKeysCreated++;
              
            } else {
              console.log('⚠️ IPinfo API request failed:', geoResponse.status);
            }
          } else {
            console.log('⚠️ IPINFO_TOKEN not configured - skipping geographic keys');
          }
        } catch (geoError) {
          console.log('⚠️ Geographic key creation failed:', geoError.message);
        }
      }
      
      const totalKeysCreated = 3 + additionalKeysCreated; // base + ip + session + additional keys
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          success: true, 
          message: 'Enhanced attribution data stored successfully',
          visitor_ip: visitorIP,
          keys_created: totalKeysCreated,
          base_key: baseKey,
          encoded_ip: encodedIP,
          enhancements: {
            fingerprint_keys: additionalKeysCreated > 0,
            direct_lookup_keys: true, // NEW: Direct lookup keys for track.js
            geographic_keys: additionalKeysCreated > 5,
            hardware_fingerprinting: true, // NEW: CPU/memory fingerprinting
            cross_device_ready: true,
            ipv6_ipv4_correlation: true
          }
        })
      };
      
    } catch (error) {
      console.error('❌ Attribution storage error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message })
      };
    }
  }
  
  if (event.httpMethod === 'GET') {
    try {
      const { ip, session_id, timestamp } = event.queryStringParameters || {};
      
      console.log('🔍 GET request params:', { ip, session_id, timestamp });
      
      if (!ip && !session_id) {
        return {
          statusCode: 400,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            error: 'Either ip or session_id parameter is required',
            found: false 
          })
        };
      }
      
      let lookupResult = null;
      let lookupMethod = 'none';
      
      // Try different lookup methods
      if (ip) {
        const encodedIP = encodeIPForKey(ip);
        console.log(`🔍 Looking up attribution for encoded IP: ${encodedIP}`);
        const ipLookupKey = `attribution_ip_${encodedIP}`;
        lookupResult = await redis(`get/${ipLookupKey}`);
        if (lookupResult.result) {
          lookupMethod = 'ip_address';
          console.log('✅ Found via IP lookup');
        }
      }
      
      if (!lookupResult?.result && session_id) {
        console.log(`🔍 Looking up attribution for session: ${session_id}`);
        const sessionLookupKey = `attribution_session_${session_id}`;
        lookupResult = await redis(`get/${sessionLookupKey}`);
        if (lookupResult.result) {
          lookupMethod = 'session_id';
          console.log('✅ Found via session lookup');
        }
      }
      
      if (!lookupResult?.result) {
        console.log('⚠️ No attribution found');
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            found: false, 
            message: 'No attribution data found',
            searched_for: { ip, session_id }
          })
        };
      }
      
      // Get the actual attribution data
      const attributionKey = lookupResult.result;
      const attributionResult = await redis(`get/${attributionKey}`);
      
      if (!attributionResult.result) {
        console.log('⚠️ Attribution key found but data missing');
        return {
          statusCode: 200,
          headers: { 'Access-Control-Allow-Origin': '*' },
          body: JSON.stringify({ 
            found: false, 
            message: 'Attribution data expired',
            key_found: attributionKey
          })
        };
      }
      
      const attributionData = JSON.parse(attributionResult.result);
      console.log('✅ Attribution data found via:', lookupMethod);
      
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ 
          found: true, 
          data: attributionData,
          lookup_method: lookupMethod,
          redis_key: attributionKey
        })
      };
      
    } catch (error) {
      console.error('❌ Attribution lookup error:', error);
      return {
        statusCode: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({ error: error.message, found: false })
      };
    }
  }
  
  return {
    statusCode: 405,
    headers: { 'Access-Control-Allow-Origin': '*' },
    body: 'Method not allowed'
  };
};

module.exports = { handler };
