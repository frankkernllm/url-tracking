// Enhanced query-pageviews-enhanced.js with IP Index Fallback
// SMART FALLBACK: Try lookup keys first, then search IP indexes for multi-signal attribution

// Replace the main attribution search logic with this cascading approach:

async function findAttributionWithFallback(redis, queryParams, windowStart, conversionTime) {
  const { ips_to_check, session_id, device_signature, screen_value, gpu_signature } = queryParams;
  let allMatches = [];
  const queryMethods = [];

  console.log('🔍 ENHANCED QUERY: Multi-signal attribution with IP index fallback');

  // PRIORITY 1: Session ID Match (300 points) - Try lookup keys first
  if (session_id) {
    console.log(`🎯 Searching session ID: ${session_id}`);
    const sessionMatches = await searchBySessionId(redis, session_id, windowStart, conversionTime);
    if (sessionMatches.length > 0) {
      sessionMatches.forEach(match => {
        match.attribution_method = 'session_id_match';
        match.confidence = 300;
      });
      allMatches = allMatches.concat(sessionMatches);
      queryMethods.push('session_id_lookup');
      console.log(`✅ Session ID: ${sessionMatches.length} matches found`);
    }
  }

  // PRIORITY 2: Device Signature Match (260 points) - Try lookup keys first  
  if (device_signature && allMatches.length === 0) {
    console.log(`🔐 Searching device signature: ${device_signature}`);
    const deviceMatches = await searchByDeviceSignature(redis, device_signature, windowStart, conversionTime);
    if (deviceMatches.length > 0) {
      deviceMatches.forEach(match => {
        match.attribution_method = 'device_signature_match';
        match.confidence = 260;
      });
      allMatches = allMatches.concat(deviceMatches);
      queryMethods.push('device_signature_lookup');
      console.log(`✅ Device signature: ${deviceMatches.length} matches found`);
    }
  }

  // PRIORITY 3: IP Address Matches (280-240 points) - Try lookup keys first
  if (ips_to_check?.length > 0 && allMatches.length === 0) {
    console.log(`📍 Searching IP addresses: ${ips_to_check.join(', ')}`);
    
    for (let i = 0; i < ips_to_check.length; i++) {
      const ip = ips_to_check[i];
      if (!ip || ip === 'unknown') continue;
      
      const ipMatches = await searchByIpAddress(redis, ip, windowStart, conversionTime);
      if (ipMatches.length > 0) {
        const confidence = i === 0 ? 280 : i === 1 ? 260 : 240;
        const ipType = i === 0 ? 'primary_ip' : i === 1 ? 'conversion_ip' : 'fallback_ip';
        
        ipMatches.forEach(match => {
          match.attribution_method = `${ipType}_match`;
          match.confidence = confidence;
        });
        
        allMatches = allMatches.concat(ipMatches);
        queryMethods.push('ip_lookup');
        console.log(`✅ IP ${ip}: ${ipMatches.length} matches found`);
        break;
      }
    }
  }

  // 🚀 NEW: FALLBACK TO IP INDEXES FOR MULTI-SIGNAL ATTRIBUTION
  if (allMatches.length === 0) {
    console.log('🔄 FALLBACK: Searching IP indexes for multi-signal attribution...');
    
    // Search IP indexes for session_id, device_signature, screen, etc.
    const indexMatches = await searchIPIndexesForMultiSignal(redis, queryParams, windowStart, conversionTime);
    if (indexMatches.length > 0) {
      allMatches = allMatches.concat(indexMatches);
      queryMethods.push('ip_index_multi_signal_fallback');
      console.log(`✅ IP Index Fallback: ${indexMatches.length} multi-signal matches found`);
    }
  }

  return { allMatches, queryMethods };
}

// NEW: Search IP indexes for multi-signal attribution
async function searchIPIndexesForMultiSignal(redis, queryParams, windowStart, conversionTime) {
  const { ips_to_check, session_id, device_signature, screen_value, gpu_signature } = queryParams;
  const matches = [];
  
  console.log('🔍 Searching IP indexes for multi-signal attribution...');
  
  // Build list of IPs to check in indexes
  const ipsToSearch = ips_to_check || [];
  
  for (const ip of ipsToSearch) {
    const encodedIP = ip.replace(/:/g, '_');
    const ipIndexKey = `pageview_index_ip:${encodedIP}`;
    
    try {
      const indexData = await redis(`get/${ipIndexKey}`);
      
      if (indexData?.result) {
        const parsed = JSON.parse(decodeURIComponent(indexData.result));
        
        // Search through pageviews in this IP index for multi-signal matches
        for (const pageview of parsed.pageviews || []) {
          const pvTime = new Date(pageview.timestamp);
          
          // Check time window
          if (pvTime >= windowStart && pvTime <= conversionTime) {
            
            // PRIORITY 1: Session ID match in IP index data (highest confidence)
            if (session_id && pageview.session_id === session_id) {
              matches.push({
                ...pageview,
                attribution_method: 'session_id_match_ip_index',
                confidence: 295, // Slightly lower than lookup key method
                matched_ip: ip,
                match_method: 'ip_index_session_match'
              });
              console.log(`✅ Found session match in IP index: ${session_id}`);
              continue; // Don't check other signals for this pageview
            }
            
            // PRIORITY 2: Device signature match in IP index data
            if (device_signature && pageview.canvas_fingerprint && 
                pageview.canvas_fingerprint.includes(device_signature)) {
              matches.push({
                ...pageview,
                attribution_method: 'device_signature_match_ip_index',
                confidence: 255, // Slightly lower than lookup key method
                matched_ip: ip,
                match_method: 'ip_index_device_match'
              });
              console.log(`✅ Found device match in IP index: ${device_signature.substring(0, 10)}...`);
              continue;
            }
            
            // PRIORITY 3: Screen signature match
            if (screen_value && pageview.screen_resolution && 
                hashString(pageview.screen_resolution) === screen_value) {
              matches.push({
                ...pageview,
                attribution_method: 'screen_signature_match_ip_index',
                confidence: 195,
                matched_ip: ip,
                match_method: 'ip_index_screen_match'
              });
              console.log(`✅ Found screen match in IP index`);
              continue;
            }
            
            // PRIORITY 4: WebGL/GPU signature match
            if (gpu_signature && pageview.webgl_fingerprint && 
                hashString(pageview.webgl_fingerprint) === gpu_signature) {
              matches.push({
                ...pageview,
                attribution_method: 'webgl_signature_match_ip_index',
                confidence: 175,
                matched_ip: ip,
                match_method: 'ip_index_webgl_match'
              });
              console.log(`✅ Found WebGL match in IP index`);
              continue;
            }
            
            // PRIORITY 5: IP-only match (fallback)
            if (!session_id && !device_signature && !screen_value && !gpu_signature) {
              matches.push({
                ...pageview,
                attribution_method: 'ip_only_match_ip_index',
                confidence: 235,
                matched_ip: ip,
                match_method: 'ip_index_ip_match'
              });
            }
          }
        }
      }
      
    } catch (error) {
      console.warn(`⚠️ IP index search failed for ${ip}: ${error.message}`);
    }
  }
  
  // Sort by confidence (highest first)
  matches.sort((a, b) => (b.confidence || 0) - (a.confidence || 0));
  
  console.log(`🔍 IP Index multi-signal search complete: ${matches.length} matches found`);
  return matches;
}

// Hash function for matching (same as store-attribution.js)
function hashString(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return Math.abs(hash).toString(36);
}
