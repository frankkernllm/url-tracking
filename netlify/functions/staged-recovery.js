// Staged Recovery System - SERVERLESS OPTIMIZED for Netlify Timeout Constraints
// Path: netlify/functions/staged-recovery.js

// Global Redis helper - accessible to all functions
let redis;

function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 3000) => { // Reduced timeout for serverless
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

// Stage a recovery without updating live data - SERVERLESS OPTIMIZED
async function stageRecovery(event, headers) {
  const startTime = Date.now();
  const maxProcessingTime = 8000; // 8 seconds max to stay under Netlify timeout
  
  try {
    const data = JSON.parse(event.body);
    
    console.log('🔄 SERVERLESS OPTIMIZED REPROCESSING:', {
      email: data.email,
      order_id: data.order_id
    });

    // Quick check if already processed in this pass
    const alreadyReprocessed2 = await checkIfAlreadyReprocessed2(data.email, data.timestamp);
    if (alreadyReprocessed2) {
      console.log(`⏭️ Already processed in second pass: ${data.email}`);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          already_processed: true,
          already_reprocessed_2: true,
          existing_recovery_id: alreadyReprocessed2.recovery_id,
          message: 'Already processed in second pass - skipping'
        })
      };
    }

    const pageviewIP = data.pageview_ip;
    const conversionIP = data.conversion_ip;
    
    console.log('📍 Quick IP Analysis:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      has_both_ips: !!(pageviewIP && conversionIP),
      ips_are_same: pageviewIP === conversionIP
    });

    // FAST attribution search with strict time limits
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
      reprocessing_attempt: 2
    };
    
    if (attributionResult) {
      const stagedRecovery = {
        recovery_id: `reprocess2_${Date.now()}_${data.email.replace('@', '_at_')}`,
        timestamp: new Date().toISOString(),
        status: 'staged',
        reprocessing: 2,
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
        recovery_method: 'fast_dual_ip_serverless',
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        matched_ip: attributionResult.matched_ip,
        matched_ip_type: attributionResult.ip_type,
        attribution_confidence: attributionResult.confidence || 'medium',
        time_diff_minutes: attributionResult.time_diff_minutes,
        needs_review: shouldFlagForReview(mockConversion, attributionResult),
        risk_level: assessRiskLevel(mockConversion, attributionResult)
      };

      // Store in staging area
      const stagingKey = `recovery_staging:${stagedRecovery.recovery_id}`;
      await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);
      
      // Mark as processed
      await markAsReprocessed2(data.email, data.timestamp, stagedRecovery.recovery_id, true);

      const totalTime = Date.now() - startTime;
      console.log(`✅ Recovery staged successfully in ${totalTime}ms`);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: true,
          reprocessed: 2,
          recovery_id: stagedRecovery.recovery_id,
          attribution_found: true,
          matched_ip: attributionResult.matched_ip,
          matched_ip_type: attributionResult.ip_type,
          time_diff_minutes: attributionResult.time_diff_minutes,
          needs_review: stagedRecovery.needs_review,
          risk_level: stagedRecovery.risk_level,
          proposed_changes: stagedRecovery.proposed_changes,
          processing_time_ms: totalTime,
          message: `Recovery staged using ${attributionResult.ip_type} IP (${attributionResult.time_diff_minutes} min before) [FAST SERVERLESS]`
        })
      };

    } else {
      console.log('❌ No attribution found in fast scan');
      
      await markAsReprocessed2(data.email, data.timestamp, null, false);
      
      const totalTime = Date.now() - startTime;
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          reprocessed: 2,
          attribution_found: false,
          conversion_found: true,
          processing_time_ms: totalTime,
          message: 'No attribution found in fast serverless scan'
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
        processing_time_ms: totalTime
      })
    };
  }
}

// FAST attribution search optimized for serverless constraints
async function findAttributionFast(pageviewIP, conversionIP, originalTimestamp, remainingTime) {
  const searchStartTime = Date.now();
  
  try {
    console.log('🚀 Fast attribution search:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      remaining_time_ms: remainingTime
    });
    
    const ipsToCheck = [];
    if (pageviewIP) ipsToCheck.push({ ip: pageviewIP, type: 'pageview' });
    if (conversionIP && conversionIP !== pageviewIP) {
      ipsToCheck.push({ ip: conversionIP, type: 'conversion' });
    }
    
    // Method 1: Direct IP lookup (fastest - try this first)
    for (const { ip, type } of ipsToCheck) {
      if (Date.now() - searchStartTime > remainingTime - 1000) {
        console.log('⏰ Time running out, skipping remaining direct lookups');
        break;
      }
      
      console.log(`🔍 Direct lookup for ${type} IP: ${ip}`);
      
      const ipKey = `attribution_ip_${encodeIPForKey(ip)}`;
      const lookupResult = await redis(`get/${ipKey}`, 1000); // 1 second timeout
      
      if (lookupResult?.result) {
        console.log(`✅ Found attribution via ${type} IP lookup key`);
        const mainKey = lookupResult.result;
        const attributionData = await redis(`get/${mainKey}`, 1000);
        
        if (attributionData?.result) {
          const attribution = JSON.parse(attributionData.result);
          
          // Verify it's within 24-hour window
          const conversionTime = new Date(originalTimestamp);
          const pageviewTime = new Date(attribution.timestamp);
          const timeDiff = conversionTime - pageviewTime;
          const twentyFourHours = 24 * 60 * 60 * 1000;
          
          if (timeDiff >= 0 && timeDiff <= twentyFourHours) {
            attribution.confidence = 'high';
            attribution.method = `${type}_ip_lookup_key_fast`;
            attribution.matched_ip = ip;
            attribution.ip_type = type;
            attribution.time_diff_minutes = Math.round(timeDiff / (1000 * 60));
            console.log(`✅ FAST direct match: ${attribution.time_diff_minutes} minutes before conversion`);
            return attribution;
          }
        }
      }
    }
    
    // Method 2: Limited scanning if we have time left
    const timeUsed = Date.now() - searchStartTime;
    const timeLeft = remainingTime - timeUsed;
    
    if (timeLeft > 2000) { // Only scan if we have at least 2 seconds left
      console.log(`🔍 Limited scanning with ${timeLeft}ms remaining`);
      
      const conversionTime = new Date(originalTimestamp);
      const windowStart = new Date(conversionTime.getTime() - (24 * 60 * 60 * 1000));
      
      const limitedPageviews = await getLimitedPageviews(windowStart, conversionTime, timeLeft - 500);
      
      // Search for IP matches
      for (const pageview of limitedPageviews) {
        for (const { ip, type } of ipsToCheck) {
          if (pageview.ip_address === ip) {
            const timeDiff = conversionTime - new Date(pageview.timestamp);
            const timeDiffMinutes = Math.round(timeDiff / (1000 * 60));
            
            console.log(`🎯 FAST IP match! ${type} IP: ${ip}, ${timeDiffMinutes} min before`);
            
            return {
              ...pageview,
              confidence: 'medium',
              method: `${type}_ip_limited_scan`,
              matched_ip: ip,
              ip_type: type,
              time_diff_minutes: timeDiffMinutes
            };
          }
        }
      }
    }
    
    console.log('❌ No matches found in fast search');
    return null;
    
  } catch (error) {
    console.error('❌ Fast attribution search error:', error);
    return null;
  }
}

// Get comprehensive pageviews optimized for 600/day volume
async function getLimitedPageviews(windowStart, windowEnd, maxTimeMs) {
  const pageviews = [];
  const scanStartTime = Date.now();
  
  try {
    console.log(`🔍 Comprehensive pageview scan with ${maxTimeMs}ms budget (optimized for ~600/day)`);
    
    let cursor = '0';
    let iterationCount = 0;
    let totalScanned = 0;
    const maxIterations = 25; // Increased for better coverage
    
    do {
      const elapsed = Date.now() - scanStartTime;
      if (elapsed > maxTimeMs || iterationCount >= maxIterations) {
        console.log(`⏰ Scan completed: ${elapsed}ms, ${iterationCount} iterations, ${totalScanned} keys scanned`);
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/100`, 2000); // Larger batches for efficiency
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      totalScanned += keys.length;
      iterationCount++;
      
      // Process main keys (skip lookup keys) in larger batches
      const mainKeys = keys.filter(key => 
        !key.includes('_ip_') && 
        !key.includes('_session_') && 
        !key.includes('_fp_') && 
        !key.includes('_screen_') && 
        !key.includes('_webgl_') && 
        !key.includes('_geo_')
      ); // Process ALL main keys, not just 10
      
      console.log(`📊 Iteration ${iterationCount}: ${keys.length} total keys, ${mainKeys.length} main keys to process`);
      
      // Process keys in parallel batches for speed
      const batchSize = 20;
      for (let i = 0; i < mainKeys.length; i += batchSize) {
        if (Date.now() - scanStartTime > maxTimeMs) break;
        
        const batch = mainKeys.slice(i, i + batchSize);
        
        // Process batch in parallel for speed
        const batchPromises = batch.map(async (key) => {
          try {
            const data = await redis(`get/${key}`, 1500); // Longer timeout for reliability
            if (data?.result) {
              const pageview = JSON.parse(data.result);
              
              if (pageview.timestamp && pageview.ip_address) {
                const pageviewTime = new Date(pageview.timestamp);
                
                if (pageviewTime >= windowStart && pageviewTime <= windowEnd) {
                  return {
                    timestamp: pageview.timestamp,
                    ip_address: pageview.ip_address,
                    landing_page: pageview.landing_page,
                    source: pageview.source,
                    utm_campaign: pageview.utm_campaign,
                    utm_medium: pageview.utm_medium,
                    utm_source: pageview.utm_source,
                    utm_term: pageview.utm_term,
                    utm_content: pageview.utm_content
                  };
                }
              }
            }
          } catch (parseError) {
            return null; // Skip malformed records
          }
          return null;
        });
        
        try {
          const batchResults = await Promise.all(batchPromises);
          const validResults = batchResults.filter(result => result !== null);
          pageviews.push(...validResults);
        } catch (batchError) {
          console.log(`⚠️ Batch processing error: ${batchError.message}`);
        }
      }
      
      // Log progress every 5 iterations
      if (iterationCount % 5 === 0) {
        const elapsed = Date.now() - scanStartTime;
        console.log(`📊 Progress: ${pageviews.length} pageviews found, ${elapsed}ms elapsed`);
      }
      
    } while (cursor !== '0' && Date.now() - scanStartTime < maxTimeMs && iterationCount < maxIterations);
    
    const totalTime = Date.now() - scanStartTime;
    console.log(`📊 Comprehensive scan complete: ${pageviews.length} pageviews found in ${totalTime}ms (${totalScanned} keys scanned)`);
    
    // Sort by timestamp (most recent first)
    pageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return pageviews;
    
  } catch (error) {
    console.error('❌ Comprehensive pageview scan error:', error);
    return pageviews;
  }
}

// Rest of the functions remain the same but with optimized timeouts...

// Review all staged recoveries
async function reviewStagedRecoveries(event, headers) {
  try {
    console.log('📋 Reviewing staged recoveries...');
    
    let cursor = '0';
    let allStagingKeys = [];
    let maxScans = 5; // Reduced for serverless
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
    for (const key of allStagingKeys.slice(0, 100)) { // Limit to prevent timeout
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

// Get global progress - optimized
async function getGlobalProgress(event, headers) {
  try {
    console.log('📊 Getting progress...');
    
    const reprocess2ProgressKey = 'recovery_reprocessing_2_progress';
    const reprocess2ProgressData = await redis(`get/${reprocess2ProgressKey}`, 2000);
    
    let reprocess2Progress = {
      total_reprocessed_2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: null,
      last_updated: null,
      last_reprocessed_2_email: null
    };
    
    if (reprocess2ProgressData?.result) {
      reprocess2Progress = JSON.parse(reprocess2ProgressData.result);
    }
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        progress: reprocess2Progress,
        attribution_rate: reprocess2Progress.total_reprocessed_2 > 0 
          ? `${((reprocess2Progress.attribution_found / reprocess2Progress.total_reprocessed_2) * 100).toFixed(1)}%`
          : '0%'
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

// Apply recovery (unchanged but with shorter timeouts)
async function applyStagedRecovery(event, headers) {
  try {
    const { recovery_id, approved_by } = JSON.parse(event.body);
    
    console.log(`🚀 Applying recovery: ${recovery_id}`);
    
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
      recovery_method: 'fast_serverless_dual_ip',
      recovery_id: recovery_id,
      approved_by: approved_by || 'system'
    };

    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(newConversion))}`, 3000);
    
    stagedRecovery.status = 'applied';
    stagedRecovery.applied_timestamp = new Date().toISOString();
    stagedRecovery.approved_by = approved_by;
    await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`, 3000);

    console.log('✅ Recovery applied');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_id: recovery_id,
        applied: true,
        updated_fields: Object.keys(stagedRecovery.proposed_changes),
        conversion_key: conversionKey,
        message: 'Recovery successfully applied'
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

// Clear staging (unchanged but optimized)
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
        message: `Staging cleared - ${deleted} removed, ${kept} kept`
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

// Helper functions
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

async function checkIfAlreadyReprocessed2(email, timestamp) {
  try {
    const reprocess2Key = `recovery_reprocess_2:${email}:${timestamp}`;
    const reprocess2Data = await redis(`get/${reprocess2Key}`, 1000);
    
    if (reprocess2Data?.result) {
      const reprocess2 = JSON.parse(reprocess2Data.result);
      return reprocess2;
    }
    
    return null;
  } catch (error) {
    console.log(`⚠️ Error checking reprocess2 status for ${email}:`, error.message);
    return null;
  }
}

async function markAsReprocessed2(email, timestamp, recoveryId, attributionFound) {
  try {
    const reprocess2Key = `recovery_reprocess_2:${email}:${timestamp}`;
    const reprocess2Entry = {
      email: email,
      timestamp: timestamp,
      reprocessed_2_at: new Date().toISOString(),
      recovery_id: recoveryId,
      attribution_found: attributionFound,
      status: 'reprocessed_2',
      reprocessing_pass: 'fast_serverless_dual_ip'
    };
    
    await redis(`set/${reprocess2Key}/${encodeURIComponent(JSON.stringify(reprocess2Entry))}`, 2000);
    
    await updateReprocessing2Progress(email, attributionFound);
    
  } catch (error) {
    console.log(`⚠️ Error marking ${email} as reprocessed2:`, error.message);
  }
}

async function updateReprocessing2Progress(email, attributionFound) {
  try {
    const reprocess2ProgressKey = 'recovery_reprocessing_2_progress';
    const progressData = await redis(`get/${reprocess2ProgressKey}`, 1000);
    
    let reprocess2Progress = {
      total_reprocessed_2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
      reprocessing_reason: 'fast_serverless_dual_ip'
    };
    
    if (progressData?.result) {
      reprocess2Progress = JSON.parse(progressData.result);
    }
    
    reprocess2Progress.total_reprocessed_2++;
    reprocess2Progress.last_updated = new Date().toISOString();
    reprocess2Progress.last_reprocessed_2_email = email;
    
    if (attributionFound) {
      reprocess2Progress.attribution_found++;
    } else {
      reprocess2Progress.no_attribution++;
    }
    
    await redis(`set/${reprocess2ProgressKey}/${encodeURIComponent(JSON.stringify(reprocess2Progress))}`, 2000);
    
    if (reprocess2Progress.total_reprocessed_2 % 10 === 0) {
      console.log(`🔄 Fast Progress: ${reprocess2Progress.total_reprocessed_2} processed, ${reprocess2Progress.attribution_found} with attribution (${((reprocess2Progress.attribution_found / reprocess2Progress.total_reprocessed_2) * 100).toFixed(1)}%)`);
    }
    
  } catch (error) {
    console.log('⚠️ Error updating reprocess2 progress:', error.message);
  }
}

function encodeIPForKey(ip) {
  return ip.replace(/:/g, '_');
}
