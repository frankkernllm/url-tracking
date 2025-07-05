// Staged Recovery System V2 - COMPLETE VERSION with Fast Pageview Queries
// Path: netlify/functions/staged-recovery-v2.js

// Global Redis helper
let redis;

function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => {
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
  // Set shorter timeout for serverless
  context.callbackWaitsForEmptyEventLoop = false;
  
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
  } else if (event.httpMethod === 'GET' && path === 'global-progress') {
    return await getGlobalProgress(event, headers);
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

// Stage a recovery using Fast Query System V2
async function stageRecovery(event, headers) {
  const startTime = Date.now();
  const maxProcessingTime = 8000; // 8 seconds max
  
  try {
    const data = JSON.parse(event.body);
    
    console.log('🔄 STAGED RECOVERY V2 (Fast Query System):', {
      email: data.email,
      order_id: data.order_id
    });

    const pageviewIP = data.pageview_ip;
    const conversionIP = data.conversion_ip;
    
    console.log('📍 IP Analysis:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      has_both_ips: !!(pageviewIP && conversionIP),
      ips_are_same: pageviewIP === conversionIP
    });

    // IMPROVED: Fast attribution search using pre-indexed data
    const attributionResult = await findAttributionFast(
      pageviewIP, 
      conversionIP, 
      data.timestamp,
      maxProcessingTime - (Date.now() - startTime)
    );
    
    const mockConversion = {
      email: data.email,
      timestamp: data.timestamp,
      order_id: data.order_id,
      source_file: data.source_file || 'csv',
      csv_row_number: data.csv_row_number,
      landing_page: null,
      source: 'direct',
      utm_campaign: null,
      utm_source: null,
      utm_medium: null,
      reprocessing_attempt: 'fast_query_v2'
    };
    
    if (attributionResult) {
      const stagedRecovery = {
        recovery_id: `fast_query_v2_${Date.now()}_${Math.random().toString(36).substr(2, 9)}_${data.email.replace('@', '_at_')}`,
        timestamp: new Date().toISOString(),
        status: 'staged',
        reprocessing: 'fast_query_v2',
        original_conversion: mockConversion,
        recovered_attribution: attributionResult,
        proposed_changes: {
          landing_page: {
            current: mockConversion.landing_page,
            proposed: attributionResult.landing_page
          },
          source: {
            current: mockConversion.source,
            proposed: attributionResult.source
          },
          utm_campaign: {
            current: mockConversion.utm_campaign,
            proposed: attributionResult.utm_campaign
          },
          utm_source: {
            current: mockConversion.utm_source,
            proposed: attributionResult.utm_source
          },
          utm_medium: {
            current: mockConversion.utm_medium,
            proposed: attributionResult.utm_medium
          }
        },
        recovery_method: 'fast_query_pre_indexed',
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        matched_ip: attributionResult.matched_ip,
        matched_ip_type: attributionResult.ip_type,
        attribution_confidence: attributionResult.confidence || 'medium',
        time_diff_minutes: attributionResult.time_diff_minutes,
        query_performance: attributionResult.query_stats || {},
        needs_review: shouldFlagForReview(mockConversion, attributionResult),
        risk_level: assessRiskLevel(mockConversion, attributionResult)
      };

      // Store in staging area
      const stagingKey = `recovery_staging:${stagedRecovery.recovery_id}`;
      await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);
      
      // Mark as processed
      await markAsProcessedV2(data.email, data.timestamp, stagedRecovery.recovery_id, true);

      const totalTime = Date.now() - startTime;
      console.log(`✅ Recovery staged successfully in ${totalTime}ms using fast query system`);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: true,
          version: 'fast_query_v2',
          recovery_id: stagedRecovery.recovery_id,
          attribution_found: true,
          matched_ip: attributionResult.matched_ip,
          matched_ip_type: attributionResult.ip_type,
          time_diff_minutes: attributionResult.time_diff_minutes,
          needs_review: stagedRecovery.needs_review,
          risk_level: stagedRecovery.risk_level,
          proposed_changes: stagedRecovery.proposed_changes,
          processing_time_ms: totalTime,
          query_performance: attributionResult.query_stats || {},
          message: `Recovery staged using ${attributionResult.method} (${attributionResult.time_diff_minutes} min before) [FAST QUERY V2]`
        })
      };

    } else {
      console.log('❌ No attribution found even with fast query system');
      
      await markAsProcessedV2(data.email, data.timestamp, null, false);
      
      const totalTime = Date.now() - startTime;
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          version: 'fast_query_v2',
          attribution_found: false,
          conversion_found: true,
          processing_time_ms: totalTime,
          message: 'No attribution found even with comprehensive fast query system [FAST QUERY V2]'
        })
      };
    }

  } catch (error) {
    const totalTime = Date.now() - startTime;
    console.error(`❌ Staging error after ${totalTime}ms:`, error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Staging failed', 
        message: error.message,
        processing_time_ms: totalTime,
        version: 'fast_query_v2'
      })
    };
  }
}

// IMPROVED: Fast attribution search using pre-indexed data
async function findAttributionFast(pageviewIP, conversionIP, originalTimestamp, remainingTime) {
  const searchStartTime = Date.now();
  
  try {
    console.log('🚀 Fast attribution search using pre-indexed data:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      remaining_time_ms: remainingTime
    });
    
    const ipsToCheck = [];
    if (pageviewIP) ipsToCheck.push(pageviewIP);
    if (conversionIP && conversionIP !== pageviewIP) {
      ipsToCheck.push(conversionIP);
    }
    
    // Method 1: Use fast pageview query system (NEW APPROACH)
    const queryResult = await queryPageviewsInWindow(pageviewIP, conversionIP, originalTimestamp);
    
    if (queryResult.success && queryResult.matches_found && queryResult.matches_found.length > 0) {
      console.log(`✅ Fast query found ${queryResult.matches_found.length} matches in ${queryResult.processing_time_ms}ms`);
      
      // Get the best match (closest to conversion time)
      const bestMatch = queryResult.matches_found[0];
      
      const conversionTime = new Date(originalTimestamp);
      const pageviewTime = new Date(bestMatch.timestamp);
      const timeDiff = conversionTime - pageviewTime;
      const timeDiffMinutes = Math.round(timeDiff / (1000 * 60));
      
      return {
        ...bestMatch,
        confidence: bestMatch.confidence || 'high',
        method: `fast_query_${bestMatch.match_method}`,
        matched_ip: bestMatch.matched_ip,
        ip_type: bestMatch.matched_ip === pageviewIP ? 'pageview' : 'conversion',
        time_diff_minutes: timeDiffMinutes,
        query_stats: {
          total_matches: queryResult.matches_found.length,
          query_time_ms: queryResult.processing_time_ms,
          methods_used: queryResult.query_methods_used
        }
      };
    }
    
    // Method 2: Fallback to original direct lookup (if fast query fails)
    console.log('⚠️ Fast query found no matches, falling back to direct lookup...');
    
    for (const ip of ipsToCheck) {
      if (Date.now() - searchStartTime > remainingTime - 1000) {
        console.log('⏰ Time running out, skipping remaining lookups');
        break;
      }
      
      console.log(`🔍 Direct lookup fallback for IP: ${ip}`);
      
      const ipKey = `attribution_ip_${encodeIPForKey(ip)}`;
      const lookupResult = await redis(`get/${ipKey}`, 1000);
      
      if (lookupResult?.result) {
        console.log(`✅ Found attribution via direct IP lookup fallback`);
        const mainKey = lookupResult.result;
        const attributionData = await redis(`get/${mainKey}`, 1000);
        
        if (attributionData?.result) {
          const attribution = JSON.parse(attributionData.result);
          
          const conversionTime = new Date(originalTimestamp);
          const pageviewTime = new Date(attribution.timestamp);
          const timeDiff = conversionTime - pageviewTime;
          const twentyFourHours = 24 * 60 * 60 * 1000;
          
          if (timeDiff >= 0 && timeDiff <= twentyFourHours) {
            attribution.confidence = 'medium';
            attribution.method = 'direct_lookup_fallback';
            attribution.matched_ip = ip;
            attribution.ip_type = ip === pageviewIP ? 'pageview' : 'conversion';
            attribution.time_diff_minutes = Math.round(timeDiff / (1000 * 60));
            console.log(`✅ Fallback direct match: ${attribution.time_diff_minutes} minutes before conversion`);
            return attribution;
          }
        }
      }
    }
    
    console.log('❌ No matches found in fast search or fallback');
    return null;
    
  } catch (error) {
    console.error('❌ Fast attribution search error:', error);
    return null;
  }
}

// NEW: Query pageviews using the fast query system
async function queryPageviewsInWindow(pageviewIP, conversionIP, conversionTimestamp) {
  try {
    const ipsToCheck = [];
    if (pageviewIP) ipsToCheck.push(pageviewIP);
    if (conversionIP && conversionIP !== pageviewIP) {
      ipsToCheck.push(conversionIP);
    }
    
    const queryPayload = {
      conversion_timestamp: conversionTimestamp,
      ips_to_check: ipsToCheck,
      window_hours: 24
    };
    
    console.log('📞 Calling fast pageview query system...');
    
    const response = await fetch('https://trackingojoy.netlify.app/.netlify/functions/query-pageviews', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': process.env.OJOY_API_KEY
      },
      body: JSON.stringify(queryPayload),
      signal: AbortSignal.timeout(5000) // 5 second timeout
    });
    
    if (!response.ok) {
      throw new Error(`Query API failed: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    
    console.log(`📊 Query result: ${result.matches_found?.length || 0} matches found`);
    console.log(`⚡ Query methods used: ${result.query_methods_used?.join(', ') || 'none'}`);
    
    return {
      success: true,
      ...result
    };
    
  } catch (error) {
    console.error('❌ Fast pageview query failed:', error);
    return {
      success: false,
      error: error.message,
      matches_found: []
    };
  }
}

// Review all staged recoveries (unchanged)
async function reviewStagedRecoveries(event, headers) {
  try {
    console.log('📋 Reviewing staged recoveries...');
    
    let cursor = '0';
    let allStagingKeys = [];
    let maxScans = 5;
    let scanCount = 0;

    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/recovery_staging:*/count/50`, 2000);
        
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

    // Load each staged recovery (limited for serverless)
    for (const key of allStagingKeys.slice(0, 100)) {
      try {
        const data = await redis(`get/${key}`, 1000);
        if (data?.result) {
          const recovery = JSON.parse(data.result);
          stagedRecoveries.push(recovery);
          
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
      
      if (aRisk !== bRisk) return bRisk - aRisk;
      return new Date(b.timestamp) - new Date(a.timestamp);
    });

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        staged_recoveries: stagedRecoveries,
        summary: summary,
        review_url: '/.netlify/functions/staged-recovery-v2/review-staged'
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

// Get global progress (updated for V2)
async function getGlobalProgress(event, headers) {
  try {
    console.log('📊 Getting V2 progress...');
    
    const progressKey = 'recovery_fast_query_v2_progress';
    const progressData = await redis(`get/${progressKey}`, 2000);
    
    let progress = {
      total_processed_v2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: null,
      last_updated: null,
      last_processed_email: null,
      processing_version: 'fast_query_pre_indexed'
    };
    
    if (progressData?.result) {
      progress = JSON.parse(progressData.result);
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        progress: progress,
        attribution_rate: progress.total_processed_v2 > 0 
          ? `${((progress.attribution_found / progress.total_processed_v2) * 100).toFixed(1)}%`
          : '0%',
        version: 'fast_query_v2'
      })
    };
    
  } catch (error) {
    console.error('❌ Progress error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Failed to get progress', message: error.message })
    };
  }
}

// Apply recovery (unchanged but with V2 metadata)
async function applyStagedRecovery(event, headers) {
  try {
    const { recovery_id, approved_by } = JSON.parse(event.body);
    
    console.log(`🚀 Applying V2 recovery: ${recovery_id}`);
    
    const stagingKey = `recovery_staging:${recovery_id}`;
    const stagingData = await redis(`get/${stagingKey}`, 2000);
    
    if (!stagingData?.result) {
      return {
        statusCode: 404,
        headers,
        body: JSON.stringify({ error: 'Staged recovery not found' })
      };
    }

    const stagedRecovery = JSON.parse(stagingData.result);
    
    const conversionKey = `conversions:${stagedRecovery.original_conversion.timestamp}:${Date.now()}`;
    const newConversion = {
      ...stagedRecovery.original_conversion,
      landing_page: stagedRecovery.recovered_attribution.landing_page,
      source: stagedRecovery.recovered_attribution.source,
      utm_campaign: stagedRecovery.recovered_attribution.utm_campaign,
      utm_source: stagedRecovery.recovered_attribution.utm_source,
      utm_medium: stagedRecovery.recovered_attribution.utm_medium,
      utm_term: stagedRecovery.recovered_attribution.utm_term,
      utm_content: stagedRecovery.recovered_attribution.utm_content,
      recovery_applied: true,
      recovery_timestamp: new Date().toISOString(),
      recovery_method: 'fast_query_v2',
      recovery_id: recovery_id,
      approved_by: approved_by || 'system'
    };

    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(newConversion))}`, 3000);
    
    stagedRecovery.status = 'applied';
    stagedRecovery.applied_timestamp = new Date().toISOString();
    stagedRecovery.approved_by = approved_by;
    await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`, 3000);

    console.log('✅ V2 Recovery applied');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_id: recovery_id,
        applied: true,
        version: 'fast_query_v2',
        updated_fields: Object.keys(stagedRecovery.proposed_changes),
        conversion_key: conversionKey,
        message: 'Recovery successfully applied using V2 system'
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

// Clear staging area (unchanged)
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

    let cursor = '0';
    let deleted = 0;
    let kept = 0;
    let maxScans = 5;
    let scanCount = 0;

    do {
      try {
        const scanResult = await redis(`scan/${cursor}/match/recovery_staging:*/count/50`, 2000);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        scanCount++;
        
        for (const key of keys) {
          if (keep_applied) {
            const data = await redis(`get/${key}`, 1000);
            if (data?.result) {
              const recovery = JSON.parse(data.result);
              if (recovery.status === 'applied') {
                kept++;
                continue;
              }
            }
          }
          
          await redis(`del/${key}`, 1000);
          deleted++;
        }
        
      } catch (scanError) {
        console.log(`❌ Clear scan error:`, scanError.message);
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
        version: 'fast_query_v2',
        message: `V2 Staging cleared - ${deleted} removed, ${kept} kept`
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

// Updated marking function for V2
async function markAsProcessedV2(email, timestamp, recoveryId, attributionFound) {
  try {
    const processKey = `recovery_fast_query_v2:${email}:${timestamp}:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`;
    const processEntry = {
      email: email,
      timestamp: timestamp,
      processed_at: new Date().toISOString(),
      recovery_id: recoveryId,
      attribution_found: attributionFound,
      status: 'processed_fast_query_v2',
      processing_version: 'fast_query_pre_indexed'
    };
    
    await redis(`set/${processKey}/${encodeURIComponent(JSON.stringify(processEntry))}`);
    
    await updateProcessingProgressV2(email, attributionFound);
    
  } catch (error) {
    console.log(`⚠️ Error marking ${email} as processed (V2):`, error.message);
  }
}

// Updated progress tracking for V2
async function updateProcessingProgressV2(email, attributionFound) {
  try {
    const progressKey = 'recovery_fast_query_v2_progress';
    const progressData = await redis(`get/${progressKey}`, 1000);
    
    let progress = {
      total_processed_v2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
      processing_version: 'fast_query_pre_indexed'
    };
    
    if (progressData?.result) {
      progress = JSON.parse(progressData.result);
    }
    
    progress.total_processed_v2++;
    progress.last_updated = new Date().toISOString();
    progress.last_processed_email = email;
    
    if (attributionFound) {
      progress.attribution_found++;
    } else {
      progress.no_attribution++;
    }
    
    await redis(`set/${progressKey}/${encodeURIComponent(JSON.stringify(progress))}`, 2000);
    
    if (progress.total_processed_v2 % 10 === 0) {
      console.log(`🔄 Fast Query V2 Progress: ${progress.total_processed_v2} processed, ${progress.attribution_found} with attribution (${((progress.attribution_found / progress.total_processed_v2) * 100).toFixed(1)}%)`);
    }
    
  } catch (error) {
    console.log('⚠️ Error updating V2 progress:', error.message);
  }
}

// Helper functions (unchanged)
function shouldFlagForReview(currentConversion, attributionResult) {
  const currentSource = currentConversion.source?.toLowerCase() || 'direct';
  const newSource = attributionResult.source?.toLowerCase() || 'unknown';
  
  if (currentSource === 'direct' && newSource !== 'direct') {
    return true;
  }
  
  if (attributionResult.landing_page?.includes('test') || 
      attributionResult.landing_page?.includes('staging')) {
    return true;
  }
  
  const timeDiff = Math.abs(new Date(currentConversion.timestamp) - new Date(attributionResult.timestamp));
  if (timeDiff > 48 * 60 * 60 * 1000) {
    return true;
  }
  
  return false;
}

function assessRiskLevel(currentConversion, attributionResult) {
  let riskScore = 0;
  
  if (attributionResult.confidence === 'high') riskScore -= 1;
  if (attributionResult.method?.includes('lookup_key')) riskScore -= 1;
  
  if (!currentConversion.landing_page && attributionResult.landing_page) riskScore += 1;
  
  if (currentConversion.source !== 'direct' && 
      attributionResult.source !== currentConversion.source) riskScore += 2;
  
  if (attributionResult.landing_page?.includes('test')) riskScore += 3;
  
  if (riskScore <= 0) return 'low';
  if (riskScore <= 2) return 'medium';
  return 'high';
}

function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_');
}
