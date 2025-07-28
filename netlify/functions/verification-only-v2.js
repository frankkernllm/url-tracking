// Verification-Only Attribution Key Recovery v2
// Path: netlify/functions/verification-only-v2.js
// Purpose: Skip main extraction, jump straight to targeted verification for missed keys

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const redis = initializeRedis();
    const startTime = Date.now();
    
    console.log('🔍 VERIFICATION-ONLY v2: Targeted recovery of missed attribution keys');
    
    // Target specific timeframes where we know missing keys exist
    const targetTimeframes = [
      '1753484*', // July 25th evening (your target IP timeframe - 23:04, 23:03)
      '1753463*', // July 25th afternoon (17:06)
      '1753465*', // July 25th late afternoon  
      '1753470*', // July 25th early evening
      '1753475*', // July 25th mid evening
      '1753480*', // July 25th late evening
      '1753485*', // July 25th very late evening
      '1753460*', // July 25th mid afternoon (broader range)
    ];
    
    console.log(`🎯 Targeting ${targetTimeframes.length} specific timeframes for verification`);
    
    // Run targeted verification
    const verificationResult = await runTargetedVerification(redis, targetTimeframes);
    
    // Store recovered keys if found
    let storageResult = null;
    if (verificationResult.recovered_pageviews.length > 0) {
      storageResult = await storeVerificationResults(redis, verificationResult.recovered_pageviews);
    }
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ VERIFICATION-ONLY v2 completed in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        verification_type: 'targeted_timeframe_recovery',
        verification_results: {
          // Key recovery stats
          target_timeframes_checked: targetTimeframes.length,
          total_keys_found: verificationResult.total_keys_found,
          recovered_pageviews: verificationResult.recovered_pageviews.length,
          target_ip_matches: verificationResult.target_ip_matches,
          processing_time_ms: totalTime,
          
          // Detailed breakdown
          timeframe_results: verificationResult.timeframe_breakdown,
          
          // Storage results
          storage_successful: storageResult ? storageResult.success : false,
          verification_chunk_key: storageResult ? storageResult.chunk_key : null
        },
        
        // Analysis of findings
        analysis: {
          missing_keys_recovered: verificationResult.target_ip_matches > 0,
          target_ip_data_found: verificationResult.target_ip_samples,
          extraction_gap_confirmed: verificationResult.total_keys_found > 0,
          verification_method: 'micro_chunk_targeted_scan'
        },
        
        next_steps: verificationResult.target_ip_matches > 0 ? [
          '🎉 SUCCESS: Missing target IP keys recovered!',
          `Found ${verificationResult.target_ip_matches} pageviews for IP 42.61.210.120`,
          'Verification data stored with v2 identifiers',
          'Gap in original extraction confirmed and resolved'
        ] : [
          '⚠️ No target IP data recovered in these timeframes',
          'Either keys were deleted or exist in different timeframes',
          'Consider broader timeframe search or different approach',
          'Original extraction may have been more complete than suspected'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Verification-only v2 failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Verification-only v2 failed', 
        message: error.message 
      })
    };
  }
};

// Run targeted verification with micro-chunking
async function runTargetedVerification(redis, targetTimeframes) {
  console.log('🔍 Starting micro-chunk targeted verification...');
  
  const verificationResults = {
    total_keys_found: 0,
    recovered_pageviews: [],
    target_ip_matches: 0,
    target_ip_samples: [],
    timeframe_breakdown: []
  };
  
  for (const timeframe of targetTimeframes) {
    console.log(`🎯 Verifying timeframe: ${timeframe}`);
    
    try {
      // Use micro-chunks (25) for maximum precision
      const timeframeKeys = await microChunkScan(redis, `attribution_*_${timeframe}`, 25);
      
      console.log(`📊 Timeframe ${timeframe}: Found ${timeframeKeys.length} keys`);
      
      if (timeframeKeys.length > 0) {
        // Process found keys
        const pageviews = await processVerificationKeys(redis, timeframeKeys);
        
        // Check for target IP matches
        const targetIPMatches = pageviews.filter(pv => pv.ip_address === '42.61.210.120');
        
        if (targetIPMatches.length > 0) {
          console.log(`🎯 TARGET IP FOUND: ${targetIPMatches.length} matches in timeframe ${timeframe}`);
          verificationResults.target_ip_matches += targetIPMatches.length;
          
          // Store samples for analysis
          targetIPMatches.forEach(match => {
            verificationResults.target_ip_samples.push({
              timestamp: match.timestamp,
              session_id: match.session_id,
              source: match.source,
              landing_page: match.landing_page,
              redis_key: match.redis_key,
              timeframe: timeframe
            });
          });
        }
        
        verificationResults.recovered_pageviews.push(...pageviews);
        verificationResults.total_keys_found += timeframeKeys.length;
        
        verificationResults.timeframe_breakdown.push({
          timeframe: timeframe,
          keys_found: timeframeKeys.length,
          pageviews_recovered: pageviews.length,
          target_ip_matches: targetIPMatches.length,
          sample_keys: timeframeKeys.slice(0, 3) // First 3 keys as samples
        });
      } else {
        verificationResults.timeframe_breakdown.push({
          timeframe: timeframe,
          keys_found: 0,
          pageviews_recovered: 0,
          target_ip_matches: 0,
          sample_keys: []
        });
      }
      
    } catch (timeframeError) {
      console.log(`⚠️ Verification error for timeframe ${timeframe}:`, timeframeError.message);
      verificationResults.timeframe_breakdown.push({
        timeframe: timeframe,
        error: timeframeError.message,
        keys_found: 0,
        pageviews_recovered: 0,
        target_ip_matches: 0
      });
    }
  }
  
  console.log(`🔍 Targeted verification complete:`);
  console.log(`   📊 Total keys found: ${verificationResults.total_keys_found}`);
  console.log(`   📝 Pageviews recovered: ${verificationResults.recovered_pageviews.length}`);
  console.log(`   🎯 Target IP matches: ${verificationResults.target_ip_matches}`);
  
  return verificationResults;
}

// Micro-chunk scan for maximum precision
async function microChunkScan(redis, pattern, microChunkSize) {
  console.log(`🔬 Micro-chunk scanning pattern: ${pattern} with chunk size: ${microChunkSize}`);
  
  const foundKeys = [];
  let cursor = '0';
  let attempts = 0;
  const maxAttempts = 50; // Safety limit
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/${microChunkSize}`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        console.log(`🏁 Micro-scan complete for pattern: ${pattern}`);
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      
      if (keys.length > 0) {
        console.log(`🔬 Micro-chunk ${attempts + 1}: Found ${keys.length} keys, cursor: ${cursor}`);
        foundKeys.push(...keys);
      }
      
      attempts++;
      if (attempts >= maxAttempts) {
        console.log(`🛑 Micro-scan safety limit reached (${maxAttempts} attempts)`);
        break;
      }
      
    } catch (scanError) {
      console.log(`⚠️ Micro-scan error: ${scanError.message}`);
      break;
    }
  } while (cursor !== '0');
  
  console.log(`🔬 Micro-scan complete: ${foundKeys.length} keys found in ${attempts} attempts`);
  return foundKeys;
}

// Process verification keys to extract pageview data
async function processVerificationKeys(redis, keys) {
  console.log(`📝 Processing ${keys.length} verification keys...`);
  
  const batchPromises = keys.map(async (key) => {
    try {
      const result = await redis(`get/${key}`, 2000);
      if (result?.result) {
        let parsed;
        try {
          parsed = JSON.parse(result.result);
        } catch (parseError) {
          try {
            parsed = JSON.parse(decodeURIComponent(result.result));
          } catch (decodeError) {
            console.log(`⚠️ Parse error for key: ${key}`);
            return null;
          }
        }
        
        if (parsed && parsed.timestamp && parsed.ip_address) {
          return {
            session_id: parsed.session_id || null,
            timestamp: parsed.timestamp,
            landing_page: parsed.landing_page || 'unknown',
            source: parsed.source || 'direct',
            ip_address: parsed.ip_address,
            canvas_fingerprint: parsed.canvas_fingerprint || null,
            webgl_fingerprint: parsed.webgl_fingerprint || null,
            referrer_url: parsed.referrer_url || null,
            utm_campaign: parsed.utm_campaign || null,
            utm_source: parsed.utm_source || null,
            utm_medium: parsed.utm_medium || null,
            screen_resolution: parsed.screen_resolution || null,
            redis_key: key,
            recovery_method: 'verification_only_v2'
          };
        }
      }
    } catch (error) {
      console.log(`⚠️ Error processing verification key ${key}: ${error.message}`);
    }
    return null;
  });
  
  const results = await Promise.all(batchPromises);
  const validPageviews = results.filter(result => result !== null);
  
  console.log(`✅ Processed verification keys: ${validPageviews.length} valid pageviews from ${keys.length} keys`);
  return validPageviews;
}

// Store verification results with v2 identifiers
async function storeVerificationResults(redis, recoveredPageviews) {
  console.log(`💾 Storing ${recoveredPageviews.length} recovered pageviews...`);
  
  const verificationChunkKey = `verification_recovery_v2:${Date.now()}`;
  
  const verificationData = {
    chunk_id: `verification_v2_${Date.now()}`,
    recovery_method: 'targeted_verification_v2',
    pageview_count: recoveredPageviews.length,
    pageviews: recoveredPageviews,
    target_ip_matches: recoveredPageviews.filter(pv => pv.ip_address === '42.61.210.120').length,
    created_at: new Date().toISOString(),
    version: 'v2_verification',
    attribution_ready: true
  };
  
  try {
    await redis(`setex/${verificationChunkKey}/2592000/${encodeURIComponent(JSON.stringify(verificationData))}`); // 30 days TTL
    console.log(`✅ Verification results stored: ${verificationChunkKey}`);
    
    return {
      success: true,
      chunk_key: verificationChunkKey,
      pageviews_stored: recoveredPageviews.length
    };
    
  } catch (storageError) {
    console.error(`❌ Failed to store verification results: ${storageError.message}`);
    return {
      success: false,
      error: storageError.message
    };
  }
}

// Initialize Redis helper
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    
    try {
      const response = await fetch(`${redisUrl}/${command}`, {
        headers: { 
          Authorization: `Bearer ${redisToken}`,
          'Content-Type': 'application/json'
        },
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`Redis error: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      clearTimeout(timeoutId);
      throw error;
    }
  };
}
