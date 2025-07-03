// Staged Recovery System - Safe Attribution Recovery with Review Process
// Path: netlify/functions/staged-recovery.js
// NO EXTERNAL DEPENDENCIES - Uses only built-in Node.js modules

// Global Redis helper - accessible to all functions
let redis;

function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 5000) => {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
      console.log(`⏰ Redis timeout after ${timeoutMs}ms for command: ${command.split('/')[0]}`);
    }, timeoutMs);
    
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
        const errorText = await response.text();
        console.log(`❌ Redis HTTP error ${response.status}: ${errorText}`);
        throw new Error(`Redis HTTP error: ${response.status} ${errorText}`);
      }
      
      const result = await response.json();
      console.log(`✅ Redis command success: ${command.split('/')[0]} -> ${JSON.stringify(result).substring(0, 100)}`);
      
      return result;
      
    } catch (error) {
      clearTimeout(timeoutId);
      
      if (error.name === 'AbortError') {
        console.log(`⏰ Redis command timed out: ${command.split('/')[0]}`);
        throw new Error(`Redis timeout after ${timeoutMs}ms`);
      }
      
      console.log(`❌ Redis command failed: ${command.split('/')[0]} - ${error.message}`);
      throw error;
    }
  };
}

exports.handler = async (event, context) => {
  // Environment variable validation
  const requiredEnvVars = [
    'UPSTASH_REDIS_REST_URL',
    'UPSTASH_REDIS_REST_TOKEN'
  ];
  
  const missingEnvVars = requiredEnvVars.filter(envVar => !process.env[envVar]);
  if (missingEnvVars.length > 0) {
    console.log('Missing environment variables:', missingEnvVars);
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
      },
      body: JSON.stringify({ 
        error: 'Configuration error',
        missing: missingEnvVars
      })
    };
  }

  // Initialize Redis function
  redis = initializeRedis();

  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  // Handle different operations
  const path = event.path.split('/').pop();
  
  if (event.httpMethod === 'POST' && path === 'stage-recovery') {
    return await stageRecovery(event, headers);
  } else if (event.httpMethod === 'GET' && path === 'review-staged') {
    return await reviewStagedRecoveries(event, headers);
  } else if (event.httpMethod === 'POST' && path === 'apply-recovery') {
    return await applyStagedRecovery(event, headers);
  } else if (event.httpMethod === 'POST' && path === 'clear-staging') {
    return await clearStagingArea(event, headers);
  }

  return {
    statusCode: 404,
    headers,
    body: JSON.stringify({ error: 'Endpoint not found' })
  };
};

// Stage a recovery without updating live data
async function stageRecovery(event, headers) {
  try {
    const data = JSON.parse(event.body);
    
    console.log('🎭 Staging recovery for:', {
      email: data.email,
      order_id: data.order_id
    });

    // Extract IPs using comprehensive mapping with fallbacks
    const pageviewIP = data.ip;
    
    // Try multiple paths for the second IP
    let conversionIP = data.checkoutview?.pageviewcheckout?.pageview?.ip;
    
    // Fallback paths if nested IP is missing
    if (!conversionIP) {
      conversionIP = data.customer?.ip_address || 
                    data.customer?.ip || 
                    data.user_ip || 
                    data.client_ip || 
                    data.conversion_ip ||
                    data.customer_ip;
    }
    
    console.log('📍 Comprehensive IP Analysis:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      nested_ip_path: data.checkoutview?.pageviewcheckout?.pageview?.ip,
      customer_ip: data.customer?.ip_address,
      has_checkoutview: !!data.checkoutview,
      has_pageviewcheckout: !!data.checkoutview?.pageviewcheckout,
      has_pageview: !!data.checkoutview?.pageviewcheckout?.pageview
    });

    // Attempt to find attribution using BOTH IPs
    const attributionResult = await findAttributionByBothIPs(pageviewIP, conversionIP, data.timestamp);
    
    // Find the current conversion record (read-only)
    const currentConversion = await findCurrentConversion(data.email, data.timestamp);
    
    if (attributionResult && currentConversion) {
      // Stage the recovery (don't update live data yet)
      const stagedRecovery = {
        // Recovery metadata
        recovery_id: `recovery_${Date.now()}_${data.email.replace('@', '_at_')}`,
        timestamp: new Date().toISOString(),
        status: 'staged',
        
        // Original data
        original_conversion: currentConversion,
        
        // What would be recovered
        recovered_attribution: attributionResult,
        
        // Proposed changes
        proposed_changes: {
          landing_page: {
            current: currentConversion.landing_page,
            proposed: attributionResult.landing_page
          },
          source: {
            current: currentConversion.source,
            proposed: attributionResult.source
          },
          utm_campaign: {
            current: currentConversion.utm_campaign,
            proposed: attributionResult.utm_campaign
          },
          utm_source: {
            current: currentConversion.utm_source,
            proposed: attributionResult.utm_source
          },
          utm_medium: {
            current: currentConversion.utm_medium,
            proposed: attributionResult.utm_medium
          }
        },
        
        // Technical details
        recovery_method: 'dual_ip_match',
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        matched_ip: attributionResult.matched_ip,
        matched_ip_type: attributionResult.ip_type,
        attribution_confidence: attributionResult.confidence || 'medium',
        
        // Validation flags
        needs_review: shouldFlagForReview(currentConversion, attributionResult),
        risk_level: assessRiskLevel(currentConversion, attributionResult)
      };

      // Store in staging area
      const stagingKey = `recovery_staging:${stagedRecovery.recovery_id}`;
      await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);
      
      // Add to staging index
      await addToStagingIndex(stagedRecovery.recovery_id, data.email);

      console.log('✅ Recovery staged successfully');

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: true,
          recovery_id: stagedRecovery.recovery_id,
          attribution_found: true,
          matched_ip: attributionResult.matched_ip,
          matched_ip_type: attributionResult.ip_type,
          needs_review: stagedRecovery.needs_review,
          risk_level: stagedRecovery.risk_level,
          proposed_changes: stagedRecovery.proposed_changes,
          message: `Recovery staged successfully using ${attributionResult.ip_type} IP - review before applying`
        })
      };

    } else {
      console.log('❌ No attribution found or conversion not found');
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          attribution_found: !!attributionResult,
          conversion_found: !!currentConversion,
          message: 'No recovery possible - no attribution found for either pageview or conversion IP, or conversion not found'
        })
      };
    }

  } catch (error) {
    console.error('❌ Staging error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Staging failed', message: error.message })
    };
  }
}

// Review all staged recoveries
async function reviewStagedRecoveries(event, headers) {
  try {
    console.log('📋 Reviewing staged recoveries...');
    
    // Use SCAN instead of KEYS to handle large datasets
    let cursor = '0';
    let allStagingKeys = [];
    let maxScans = 10;
    let scanCount = 0;

    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/recovery_staging:*/count/50`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        allStagingKeys = allStagingKeys.concat(keys);
        scanCount++;
        
      } catch (scanError) {
        console.log(`❌ Review staging scan error:`, scanError.message);
        break;
      }
      
    } while (cursor !== '0' && scanCount < maxScans);
    
    if (allStagingKeys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged_recoveries: [],
          total: 0,
          message: 'No staged recoveries found'
        })
      };
    }

    const stagedRecoveries = [];
    const summary = {
      total: 0,
      needs_review: 0,
      low_risk: 0,
      medium_risk: 0,
      high_risk: 0,
      by_source: {}
    };

    // Load each staged recovery
    for (const key of allStagingKeys) {
      try {
        const data = await redis(`get/${key}`);
        if (data?.result) {
          const recovery = JSON.parse(data.result);
          stagedRecoveries.push(recovery);
          
          // Update summary
          summary.total++;
          if (recovery.needs_review) summary.needs_review++;
          summary[recovery.risk_level + '_risk']++;
          
          const source = recovery.recovered_attribution?.source || 'unknown';
          summary.by_source[source] = (summary.by_source[source] || 0) + 1;
        }
      } catch (parseError) {
        console.log(`⚠️ Error parsing ${key}:`, parseError.message);
      }
    }

    // Sort by risk level and timestamp
    stagedRecoveries.sort((a, b) => {
      const riskOrder = { high: 3, medium: 2, low: 1 };
      const aRisk = riskOrder[a.risk_level] || 0;
      const bRisk = riskOrder[b.risk_level] || 0;
      
      if (aRisk !== bRisk) return bRisk - aRisk; // High risk first
      return new Date(b.timestamp) - new Date(a.timestamp); // Then by newest
    });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        staged_recoveries: stagedRecoveries,
        summary: summary,
        review_url: '/.netlify/functions/staged-recovery/review-staged'
      })
    };

  } catch (error) {
    console.error('❌ Review error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Review failed', message: error.message })
    };
  }
}

// Apply a specific staged recovery to live data
async function applyStagedRecovery(event, headers) {
  try {
    const { recovery_id, approved_by } = JSON.parse(event.body);
    
    console.log(`🚀 Applying staged recovery: ${recovery_id}`);
    
    // Load staged recovery
    const stagingKey = `recovery_staging:${recovery_id}`;
    const stagingData = await redis(`get/${stagingKey}`);
    
    if (!stagingData?.result) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ error: 'Staged recovery not found' })
      };
    }

    const stagedRecovery = JSON.parse(stagingData.result);
    
    // Apply the recovery to live data
    const conversionKey = stagedRecovery.original_conversion.key;
    const updatedConversion = {
      ...stagedRecovery.original_conversion,
      
      // Apply recovered attribution
      landing_page: stagedRecovery.recovered_attribution.landing_page,
      source: stagedRecovery.recovered_attribution.source,
      utm_campaign: stagedRecovery.recovered_attribution.utm_campaign,
      utm_source: stagedRecovery.recovered_attribution.utm_source,
      utm_medium: stagedRecovery.recovered_attribution.utm_medium,
      utm_term: stagedRecovery.recovered_attribution.utm_term,
      utm_content: stagedRecovery.recovered_attribution.utm_content,
      
      // Recovery metadata
      recovery_applied: true,
      recovery_timestamp: new Date().toISOString(),
      recovery_method: 'staged_dual_ip_match',
      recovery_id: recovery_id,
      approved_by: approved_by || 'system',
      pageview_ip_used: stagedRecovery.pageview_ip,
      conversion_ip_used: stagedRecovery.conversion_ip,
      matched_ip: stagedRecovery.matched_ip,
      matched_ip_type: stagedRecovery.matched_ip_type
    };

    // Update live conversion data
    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(updatedConversion))}`);
    
    // Mark staged recovery as applied
    stagedRecovery.status = 'applied';
    stagedRecovery.applied_timestamp = new Date().toISOString();
    stagedRecovery.approved_by = approved_by;
    await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);

    console.log('✅ Recovery applied to live data');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_id: recovery_id,
        applied: true,
        updated_fields: Object.keys(stagedRecovery.proposed_changes),
        message: 'Recovery successfully applied to live conversion data'
      })
    };

  } catch (error) {
    console.error('❌ Apply error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Apply failed', message: error.message })
    };
  }
}

// Clear staging area (for cleanup)
async function clearStagingArea(event, headers) {
  try {
    const { confirm, keep_applied } = JSON.parse(event.body || '{}');
    
    if (confirm !== 'yes') {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ error: 'Must confirm with "confirm": "yes"' })
      };
    }

    // Use SCAN instead of KEYS to handle large datasets
    let cursor = '0';
    let deleted = 0;
    let kept = 0;
    let maxScans = 10;
    let scanCount = 0;

    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/recovery_staging:*/count/50`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        scanCount++;
        
        for (const key of keys) {
          if (keep_applied) {
            // Check if this recovery was applied
            const data = await redis(`get/${key}`);
            if (data?.result) {
              const recovery = JSON.parse(data.result);
              if (recovery.status === 'applied') {
                kept++;
                continue;
              }
            }
          }
          
          await redis(`del/${key}`);
          deleted++;
        }
        
      } catch (scanError) {
        console.log(`❌ Clear staging scan error:`, scanError.message);
        break;
      }
      
    } while (cursor !== '0' && scanCount < maxScans);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        deleted: deleted,
        kept: kept,
        message: `Staging area cleared - ${deleted} removed, ${kept} kept`
      })
    };

  } catch (error) {
    console.error('❌ Clear error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Clear failed', message: error.message })
    };
  }
}

// Find attribution using BOTH IP addresses
async function findAttributionByBothIPs(pageviewIP, conversionIP, originalTimestamp) {
  try {
    console.log('🔍 Searching for attribution with BOTH IPs:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP
    });
    
    const ipsToCheck = [];
    
    // Add both IPs to check list (remove duplicates)
    if (pageviewIP) ipsToCheck.push({ ip: pageviewIP, type: 'pageview' });
    if (conversionIP && conversionIP !== pageviewIP) {
      ipsToCheck.push({ ip: conversionIP, type: 'conversion' });
    }
    
    console.log(`🎯 Will check ${ipsToCheck.length} unique IP(s) for attribution data`);
    
    // Try each IP for attribution data
    for (const { ip, type } of ipsToCheck) {
      console.log(`🔍 Trying ${type} IP: ${ip}`);
      
      // Method 1: Try IP lookup keys
      const ipKey = `attribution_ip_${encodeIPForKey(ip)}`;
      let lookupResult = await redis(`get/${ipKey}`);
      
      if (lookupResult?.result) {
        console.log(`✅ Found attribution via ${type} IP lookup key`);
        const mainKey = lookupResult.result;
        const attributionData = await redis(`get/${mainKey}`);
        if (attributionData?.result) {
          const attribution = JSON.parse(attributionData.result);
          attribution.confidence = 'high';
          attribution.method = `${type}_ip_lookup_key`;
          attribution.matched_ip = ip;
          attribution.ip_type = type;
          return attribution;
        }
      } else {
        console.log(`⚠️ No lookup key found for ${type} IP: ${ipKey}`);
      }
    }
    
    // Method 2: Quick scan with early timeout (optimized for speed)
    console.log('🔍 Fallback: Quick scan for IP matches (limited scope)...');
    
    let cursor = '0';
    let totalScanned = 0;
    let maxScans = 5; // Much smaller limit to prevent timeouts
    let scanCount = 0;
    let startTime = Date.now();
    
    do {
      try {
        // Check timeout (max 5 seconds for scanning)
        if (Date.now() - startTime > 5000) {
          console.log(`⏰ Scan timeout after ${scanCount} iterations, ${totalScanned} keys`);
          break;
        }
        
        const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/50`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          console.log('⚠️ Invalid scan result, breaking');
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        totalScanned += keys.length;
        scanCount++;
        
        console.log(`🔍 Quick scan ${scanCount}: Found ${keys.length} keys, cursor: ${cursor}, total: ${totalScanned}`);
        
        // Check only main attribution keys (skip lookup keys entirely)
        for (const key of keys) {
          // Skip ALL lookup keys to focus on main data
          if (key.includes('_ip_') || key.includes('_session_') || key.includes('_fp_') || 
              key.includes('_screen_') || key.includes('_webgl_') || key.includes('_geo_')) {
            continue;
          }
          
          try {
            const data = await redis(`get/${key}`);
            if (data?.result) {
              const attribution = JSON.parse(data.result);
              
              // Check if this attribution record matches any of our IPs
              for (const { ip, type } of ipsToCheck) {
                if (attribution.ip_address === ip) {
                  console.log(`🎯 IP MATCH FOUND! ${type} IP: ${ip} in key: ${key}`);
                  
                  // Quick timeframe check (within 3 days for initial testing)
                  const timeDiff = Math.abs(new Date(originalTimestamp) - new Date(attribution.timestamp));
                  const threeDaysMs = 3 * 24 * 60 * 60 * 1000;
                  
                  if (timeDiff < threeDaysMs) {
                    console.log(`✅ Found matching attribution via ${type} IP scan:`, {
                      key: key,
                      matched_ip: ip,
                      landing_page: attribution.landing_page,
                      source: attribution.source,
                      scan_count: scanCount,
                      total_scanned: totalScanned,
                      time_diff_hours: Math.round(timeDiff / (1000 * 60 * 60))
                    });
                    attribution.confidence = 'medium';
                    attribution.method = `${type}_ip_scan_match`;
                    attribution.matched_ip = ip;
                    attribution.ip_type = type;
                    return attribution;
                  } else {
                    console.log(`⏰ IP match found but outside time window: ${Math.round(timeDiff / (1000 * 60 * 60))} hours`);
                  }
                }
              }
            }
          } catch (parseError) {
            // Skip malformed records silently
            continue;
          }
        }
        
      } catch (scanError) {
        console.log(`❌ Scan error on iteration ${scanCount}:`, scanError.message);
        break;
      }
      
    } while (cursor !== '0' && scanCount < maxScans && (Date.now() - startTime) < 5000);
    
    console.log(`🔍 Quick scan complete: ${scanCount} iterations, ${totalScanned} keys, ${Date.now() - startTime}ms`);
    console.log('❌ No matching attribution found in quick scan');
    return null;
    
  } catch (error) {
    console.error('❌ Error finding attribution:', error);
    return null;
  }
}

async function findCurrentConversion(email, timestamp) {
  try {
    // Use SCAN instead of KEYS to handle large datasets
    let cursor = '0';
    let totalScanned = 0;
    let maxScans = 20; // Limit for conversion search
    let scanCount = 0;
    
    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/50`);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          console.log('⚠️ Conversion scan: Invalid result, breaking');
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        totalScanned += keys.length;
        scanCount++;
        
        console.log(`🔍 Conversion scan ${scanCount}: Found ${keys.length} keys, total: ${totalScanned}`);
        
        // Check each conversion key
        for (const key of keys) {
          try {
            const conversionData = await redis(`get/${key}`);
            if (conversionData?.result) {
              const conversion = JSON.parse(conversionData.result);
              
              if (conversion.email === email && conversion.timestamp === timestamp) {
                conversion.key = key; // Store the key for later updates
                console.log(`✅ Found conversion: ${email} in ${key}`);
                return conversion;
              }
            }
          } catch (parseError) {
            // Skip malformed records
            continue;
          }
        }
        
      } catch (scanError) {
        console.log(`❌ Conversion scan error on iteration ${scanCount}:`, scanError.message);
        break;
      }
      
    } while (cursor !== '0' && scanCount < maxScans);
    
    console.log(`🔍 Conversion scan complete: ${scanCount} iterations, ${totalScanned} keys total`);
    console.log(`❌ No conversion found for ${email}`);
    return null;
    
  } catch (error) {
    console.error('❌ Error finding conversion:', error);
    return null;
  }
}

function shouldFlagForReview(currentConversion, attributionResult) {
  // Flag for review if:
  
  // 1. Significant change in source
  const currentSource = currentConversion.source?.toLowerCase() || 'direct';
  const newSource = attributionResult.source?.toLowerCase() || 'unknown';
  
  if (currentSource === 'direct' && newSource !== 'direct') {
    return true; // Direct -> Attributed source
  }
  
  // 2. Landing page seems suspicious
  if (attributionResult.landing_page?.includes('test') || 
      attributionResult.landing_page?.includes('staging')) {
    return true;
  }
  
  // 3. Attribution is older than 48 hours
  const timeDiff = Math.abs(new Date(currentConversion.timestamp) - new Date(attributionResult.timestamp));
  if (timeDiff > 48 * 60 * 60 * 1000) {
    return true;
  }
  
  return false;
}

function assessRiskLevel(currentConversion, attributionResult) {
  let riskScore = 0;
  
  // Low risk indicators
  if (attributionResult.confidence === 'high') riskScore -= 1;
  if (attributionResult.method === 'ip_lookup_key') riskScore -= 1;
  
  // Medium risk indicators
  if (!currentConversion.landing_page && attributionResult.landing_page) riskScore += 1;
  
  // High risk indicators
  if (currentConversion.source !== 'direct' && 
      attributionResult.source !== currentConversion.source) riskScore += 2;
  
  if (attributionResult.landing_page?.includes('test')) riskScore += 3;
  
  if (riskScore <= 0) return 'low';
  if (riskScore <= 2) return 'medium';
  return 'high';
}

async function addToStagingIndex(recoveryId, email) {
  try {
    const indexKey = 'recovery_staging_index';
    const indexEntry = { recovery_id: recoveryId, email: email, timestamp: new Date().toISOString() };
    
    // This is a simple approach - in production you might want a more sophisticated index
    await redis(`set/${indexKey}:${recoveryId}/${encodeURIComponent(JSON.stringify(indexEntry))}`);
  } catch (error) {
    console.log('⚠️ Failed to update staging index:', error.message);
  }
}

function encodeIPForKey(ip) {
  // Replace both colons (IPv6) and dots (IPv4) with underscores
  return ip.replace(/[:.]/g, '_');
}
