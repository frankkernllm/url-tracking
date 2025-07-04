// Staged Recovery System - Updated for Clean CSV Data with Caching and SECOND Reprocessing Pass
// Path: netlify/functions/staged-recovery.js

// Global Redis helper - accessible to all functions
let redis;

// Global cache for pageviews (stored in memory during processing session)
let pageviewCache = new Map();
let cacheStats = {
  hits: 0,
  misses: 0,
  total_pageviews_cached: 0
};

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

// Stage a recovery without updating live data - UPDATED FOR SECOND REPROCESSING PASS
async function stageRecovery(event, headers) {
  try {
    const data = JSON.parse(event.body);
    
    console.log('🔄 SECOND REPROCESSING PASS for comprehensive attribution:', {
      email: data.email,
      order_id: data.order_id
    });

    // Check if this conversion has already been processed in THIS second pass
    const alreadyReprocessed2 = await checkIfAlreadyReprocessed2(data.email, data.timestamp);
    if (alreadyReprocessed2) {
      console.log(`⏭️ Conversion already processed in SECOND PASS: ${data.email}`);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          already_processed: true,
          already_reprocessed_2: true,
          existing_recovery_id: alreadyReprocessed2.recovery_id,
          message: 'Conversion already processed in second reprocessing pass - skipping'
        })
      };
    }

    // Log previous processing status (but don't skip - process anyway)
    const previouslyProcessed = await checkIfAlreadyProcessed(data.email, data.timestamp);
    const previouslyReprocessed = await checkIfAlreadyReprocessed(data.email, data.timestamp);
    
    if (previouslyReprocessed) {
      console.log(`🔄 SECOND PASS: Previously reprocessed conversion: ${data.email} (comprehensive re-evaluation)`);
    } else if (previouslyProcessed) {
      console.log(`🔄 SECOND PASS: Previously processed conversion: ${data.email} (first reprocessing)`);
    } else {
      console.log(`🔄 SECOND PASS: New conversion: ${data.email} (first time processing)`);
    }

    // SIMPLIFIED IP extraction for clean CSV data
    const pageviewIP = data.pageview_ip;
    const conversionIP = data.conversion_ip;
    
    console.log('📍 Clean IP Analysis (Second Pass):', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      has_both_ips: !!(pageviewIP && conversionIP),
      ips_are_same: pageviewIP === conversionIP,
      previous_processing: !!previouslyProcessed,
      previous_reprocessing: !!previouslyReprocessed
    });

    // Attempt to find attribution using 24-hour window approach with caching
    const attributionResult = await findAttributionByBothIPsWithTimeout(
      pageviewIP, 
      conversionIP, 
      data.timestamp
    );
    
    // For CSV data, we create a mock conversion record since we don't look up existing ones
    const mockConversion = {
      email: data.email,
      timestamp: data.timestamp,
      order_id: data.order_id,
      source_file: data.source_file || 'csv',
      csv_row_number: data.csv_row_number,
      // Assume minimal current attribution for CSV data
      landing_page: null,
      source: 'direct',
      utm_campaign: null,
      utm_source: null,
      utm_medium: null,
      // Mark as second reprocessing attempt
      reprocessing_attempt: 2,
      previously_processed: !!previouslyProcessed,
      previously_reprocessed: !!previouslyReprocessed
    };
    
    if (attributionResult) {
      // Stage the recovery (don't update live data yet)
      const stagedRecovery = {
        // Recovery metadata
        recovery_id: `reprocess2_${Date.now()}_${data.email.replace('@', '_at_')}`,
        timestamp: new Date().toISOString(),
        status: 'staged',
        
        // Mark this as a second reprocessing recovery
        reprocessing: 2,
        previously_processed: !!previouslyProcessed,
        previously_reprocessed: !!previouslyReprocessed,
        
        // Original data (mock for CSV)
        original_conversion: mockConversion,
        
        // What would be recovered
        recovered_attribution: attributionResult,
        
        // Proposed changes
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
        
        // Technical details
        recovery_method: 'dual_ip_24h_window_reprocess_2',
        pageview_ip: pageviewIP,
        conversion_ip: conversionIP,
        matched_ip: attributionResult.matched_ip,
        matched_ip_type: attributionResult.ip_type,
        attribution_confidence: attributionResult.confidence || 'medium',
        time_diff_minutes: attributionResult.time_diff_minutes,
        
        // Validation flags
        needs_review: shouldFlagForReview(mockConversion, attributionResult),
        risk_level: assessRiskLevel(mockConversion, attributionResult)
      };

      // Store in staging area
      const stagingKey = `recovery_staging:${stagedRecovery.recovery_id}`;
      await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);
      
      // Add to staging index
      await addToStagingIndex(stagedRecovery.recovery_id, data.email);

      // Mark as processed in SECOND REPROCESSING PASS
      await markAsReprocessed2(data.email, data.timestamp, stagedRecovery.recovery_id, true);

      console.log('✅ Recovery staged successfully (SECOND REPROCESSING PASS)');

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
          message: `Recovery staged successfully using ${attributionResult.ip_type} IP (${attributionResult.time_diff_minutes} min before conversion) [SECOND REPROCESSING PASS]`
        })
      };

    } else {
      console.log('❌ No attribution found for either IP in 24-hour window (SECOND REPROCESSING PASS)');
      
      // Mark as processed in SECOND REPROCESSING PASS even if no attribution found
      await markAsReprocessed2(data.email, data.timestamp, null, false);
      
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          staged: false,
          reprocessed: 2,
          attribution_found: false,
          conversion_found: true, // We always have CSV data
          message: 'No recovery possible - no attribution found for either pageview or conversion IP within 24-hour window [SECOND REPROCESSING PASS]'
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

// Enhanced find attribution with intelligent caching
async function findAttributionByBothIPsWithTimeout(pageviewIP, conversionIP, originalTimestamp) {
  try {
    console.log('🔍 24-Hour Window Attribution Search with Caching:', {
      pageview_ip: pageviewIP,
      conversion_ip: conversionIP,
      conversion_time: originalTimestamp
    });
    
    const ipsToCheck = [];
    
    // Add both IPs to check list (remove duplicates)
    if (pageviewIP) ipsToCheck.push({ ip: pageviewIP, type: 'pageview' });
    if (conversionIP && conversionIP !== pageviewIP) {
      ipsToCheck.push({ ip: conversionIP, type: 'conversion' });
    }
    
    console.log(`🎯 Will check ${ipsToCheck.length} unique IP(s) for attribution data`);
    
    // Method 1: Try direct IP lookup keys first (fastest)
    for (const { ip, type } of ipsToCheck) {
      console.log(`🔍 Trying direct lookup for ${type} IP: ${ip}`);
      
      const ipKey = `attribution_ip_${encodeIPForKey(ip)}`;
      let lookupResult = await redis(`get/${ipKey}`);
      
      if (lookupResult?.result) {
        console.log(`✅ Found attribution via ${type} IP lookup key`);
        const mainKey = lookupResult.result;
        const attributionData = await redis(`get/${mainKey}`);
        if (attributionData?.result) {
          const attribution = JSON.parse(attributionData.result);
          
          // Verify it's within 24-hour window
          const conversionTime = new Date(originalTimestamp);
          const pageviewTime = new Date(attribution.timestamp);
          const timeDiff = conversionTime - pageviewTime;
          const twentyFourHours = 24 * 60 * 60 * 1000;
          
          if (timeDiff >= 0 && timeDiff <= twentyFourHours) {
            attribution.confidence = 'high';
            attribution.method = `${type}_ip_lookup_key`;
            attribution.matched_ip = ip;
            attribution.ip_type = type;
            attribution.time_diff_minutes = Math.round(timeDiff / (1000 * 60));
            console.log(`✅ Direct match within 24h window: ${attribution.time_diff_minutes} minutes before conversion`);
            return attribution;
          } else {
            console.log(`⏰ Direct match found but outside 24h window: ${Math.round(timeDiff / (1000 * 60 * 60))} hours`);
          }
        }
      }
    }
    
    // Method 2: 24-hour window pageview scanning with caching
    console.log('🔍 Scanning pageviews within 24-hour window (with caching)...');
    
    const conversionTime = new Date(originalTimestamp);
    const windowStart = new Date(conversionTime.getTime() - (24 * 60 * 60 * 1000));
    
    console.log(`📅 24-hour window: ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
    
    const pageviewsInWindow = await getCachedPageviewsInTimeWindow(windowStart, conversionTime);
    
    if (pageviewsInWindow.length === 0) {
      console.log('❌ No pageviews found in 24-hour window');
      return null;
    }
    
    console.log(`📊 Found ${pageviewsInWindow.length} pageviews in 24-hour window (cache stats: ${cacheStats.hits} hits, ${cacheStats.misses} misses)`);
    
    // Search pageviews for IP matches
    for (const pageview of pageviewsInWindow) {
      for (const { ip, type } of ipsToCheck) {
        if (pageview.ip_address === ip) {
          const timeDiff = conversionTime - new Date(pageview.timestamp);
          const timeDiffMinutes = Math.round(timeDiff / (1000 * 60));
          
          console.log(`🎯 IP MATCH in 24h window! ${type} IP: ${ip}`);
          console.log(`   Time difference: ${timeDiffMinutes} minutes before conversion`);
          console.log(`   Landing page: ${pageview.landing_page}`);
          console.log(`   Source: ${pageview.source}`);
          
          return {
            ...pageview,
            confidence: 'high',
            method: `${type}_ip_24h_window_match`,
            matched_ip: ip,
            ip_type: type,
            time_diff_minutes: timeDiffMinutes
          };
        }
      }
    }
    
    console.log('❌ No IP matches found in 24-hour window pageviews');
    return null;
    
  } catch (error) {
    console.error('❌ Error in 24-hour window attribution search:', error);
    return null;
  }
}

// Cached pageview retrieval with intelligent time window management
async function getCachedPageviewsInTimeWindow(windowStart, windowEnd) {
  // Create cache key based on date (cache by day for efficiency)
  const startDate = windowStart.toISOString().split('T')[0]; // YYYY-MM-DD
  const endDate = windowEnd.toISOString().split('T')[0];
  
  // Generate list of dates we need to check (usually 1-2 days for 24h window)
  const datesNeeded = [];
  let currentDate = new Date(startDate);
  const finalDate = new Date(endDate);
  
  while (currentDate <= finalDate) {
    datesNeeded.push(currentDate.toISOString().split('T')[0]);
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  console.log(`📅 Need pageviews for dates: ${datesNeeded.join(', ')}`);
  
  let allPageviews = [];
  
  // Check cache for each date needed
  for (const date of datesNeeded) {
    if (pageviewCache.has(date)) {
      // Cache hit
      cacheStats.hits++;
      const cachedPageviews = pageviewCache.get(date);
      allPageviews = allPageviews.concat(cachedPageviews);
      console.log(`💾 Cache HIT for ${date}: ${cachedPageviews.length} pageviews`);
    } else {
      // Cache miss - need to fetch and cache this date
      cacheStats.misses++;
      console.log(`🔍 Cache MISS for ${date}: scanning Redis...`);
      
      const datePageviews = await scanPageviewsForDate(date);
      pageviewCache.set(date, datePageviews);
      allPageviews = allPageviews.concat(datePageviews);
      cacheStats.total_pageviews_cached += datePageviews.length;
      
      console.log(`💾 Cached ${datePageviews.length} pageviews for ${date}`);
    }
  }
  
  // Filter to exact time window
  const filteredPageviews = allPageviews.filter(pageview => {
    const pageviewTime = new Date(pageview.timestamp);
    return pageviewTime >= windowStart && pageviewTime <= windowEnd;
  });
  
  console.log(`📊 Cache Summary: ${cacheStats.hits} hits, ${cacheStats.misses} misses, ${cacheStats.total_pageviews_cached} total cached`);
  console.log(`🎯 Filtered to ${filteredPageviews.length} pageviews in exact time window`);
  
  // Sort pageviews by timestamp (most recent first)
  filteredPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  
  return filteredPageviews;
}

// Scan and cache pageviews for a specific date
async function scanPageviewsForDate(date) {
  const pageviews = [];
  const startTime = Date.now();
  const maxScanTime = 120000; // 2 minutes max for comprehensive scanning (you only have ~600/day)
  
  try {
    console.log(`🔍 Comprehensive scan for date: ${date}`);
    
    let cursor = '0';
    let totalScanned = 0;
    let maxIterations = 200; // Increased for comprehensive coverage
    let iterationCount = 0;
    
    do {
      // Check timeout before each scan
      const elapsed = Date.now() - startTime;
      if (elapsed > maxScanTime) {
        console.log(`⏰ Comprehensive scan timeout after ${elapsed}ms, ${totalScanned} keys scanned`);
        break;
      }
      
      if (iterationCount >= maxIterations) {
        console.log(`🔄 Reached max iterations (${maxIterations}), stopping scan`);
        break;
      }
      
      try {
        const scanResult = await redis(`scan/${cursor}/match/attribution_*/count/100`); // Increased batch size
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        totalScanned += keys.length;
        iterationCount++;
        
        // Process keys in batches
        const batchSize = 50; // Increased for efficiency
        for (let i = 0; i < keys.length; i += batchSize) {
          // Check timeout during batch processing
          if (Date.now() - startTime > maxScanTime) {
            console.log(`⏰ Timeout during batch processing`);
            return pageviews;
          }
          
          const batch = keys.slice(i, i + batchSize);
          
          // Skip lookup keys to focus on main pageview data
          const mainKeys = batch.filter(key => 
            !key.includes('_ip_') && 
            !key.includes('_session_') && 
            !key.includes('_fp_') && 
            !key.includes('_screen_') && 
            !key.includes('_webgl_') && 
            !key.includes('_geo_')
          );
          
          for (const key of mainKeys) {
            try {
              const data = await redis(`get/${key}`);
              if (data?.result) {
                const pageview = JSON.parse(data.result);
                
                // Check if pageview is for our target date
                if (pageview.timestamp && pageview.ip_address) {
                  const pageviewDate = pageview.timestamp.split('T')[0]; // YYYY-MM-DD
                  
                  if (pageviewDate === date) {
                    pageviews.push({
                      timestamp: pageview.timestamp,
                      ip_address: pageview.ip_address,
                      landing_page: pageview.landing_page,
                      source: pageview.source,
                      utm_campaign: pageview.utm_campaign,
                      utm_medium: pageview.utm_medium,
                      utm_source: pageview.utm_source,
                      utm_term: pageview.utm_term,
                      utm_content: pageview.utm_content,
                      redis_key: key
                    });
                  }
                }
              }
            } catch (parseError) {
              // Skip malformed records
              continue;
            }
          }
        }
        
        // Log progress every 20 iterations
        if (iterationCount % 20 === 0) {
          const elapsed = Date.now() - startTime;
          console.log(`📊 Date ${date} - Iteration ${iterationCount}: ${totalScanned} keys scanned, ${pageviews.length} pageviews found (${elapsed}ms)`);
        }
        
      } catch (scanError) {
        console.log(`❌ Scan error on iteration ${iterationCount}:`, scanError.message);
        break;
      }
      
    } while (cursor !== '0' && Date.now() - startTime < maxScanTime && iterationCount < maxIterations);
    
    const totalElapsed = Date.now() - startTime;
    console.log(`✅ Date ${date} scan complete: ${totalScanned} keys scanned, ${pageviews.length} pageviews found in ${totalElapsed}ms`);
    
    return pageviews;
    
  } catch (error) {
    console.error(`❌ Error scanning pageviews for date ${date}:`, error);
    return pageviews; // Return what we found so far
  }
}

// Review all staged recoveries (unchanged)
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

// Get global progress for resume capability - UPDATED FOR SECOND REPROCESSING
async function getGlobalProgress(event, headers) {
  try {
    console.log('📊 Getting global progress (including second reprocessing pass)...');
    
    // Get original progress
    const globalProgressKey = 'recovery_global_progress';
    const originalProgressData = await redis(`get/${globalProgressKey}`);
    
    let originalProgress = {
      total_processed: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: null,
      last_updated: null,
      last_processed_email: null
    };
    
    if (originalProgressData?.result) {
      originalProgress = JSON.parse(originalProgressData.result);
    }
    
    // Get first reprocessing progress
    const reprocessProgressKey = 'recovery_reprocessing_progress';
    const reprocessProgressData = await redis(`get/${reprocessProgressKey}`);
    
    let reprocessProgress = {
      total_reprocessed: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: null,
      last_updated: null,
      last_reprocessed_email: null
    };
    
    if (reprocessProgressData?.result) {
      reprocessProgress = JSON.parse(reprocessProgressData.result);
    }
    
    // Get second reprocessing progress
    const reprocess2ProgressKey = 'recovery_reprocessing_2_progress';
    const reprocess2ProgressData = await redis(`get/${reprocess2ProgressKey}`);
    
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
        progress: reprocess2Progress.total_reprocessed_2 > 0 ? reprocess2Progress : 
                  reprocessProgress.total_reprocessed > 0 ? reprocessProgress : originalProgress,
        reprocessing_progress: reprocessProgress,
        reprocessing_2_progress: reprocess2Progress,
        attribution_rate: reprocess2Progress.total_reprocessed_2 > 0 
          ? `${((reprocess2Progress.attribution_found / reprocess2Progress.total_reprocessed_2) * 100).toFixed(1)}%`
          : reprocessProgress.total_reprocessed > 0 
            ? `${((reprocessProgress.attribution_found / reprocessProgress.total_reprocessed) * 100).toFixed(1)}%`
            : originalProgress.total_processed > 0 
              ? `${((originalProgress.attribution_found / originalProgress.total_processed) * 100).toFixed(1)}%`
              : '0%'
      })
    };
    
  } catch (error) {
    console.error('❌ Global progress error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ error: 'Failed to get global progress', message: error.message })
    };
  }
}

// Apply a specific staged recovery to live data (unchanged)
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
    
    // For CSV data, we create a new conversion record instead of updating existing one
    const conversionKey = `conversions:${stagedRecovery.original_conversion.timestamp}:${Date.now()}`;
    const newConversion = {
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

    // Store new conversion record
    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(newConversion))}`);
    
    // Mark staged recovery as applied
    stagedRecovery.status = 'applied';
    stagedRecovery.applied_timestamp = new Date().toISOString();
    stagedRecovery.approved_by = approved_by;
    await redis(`set/${stagingKey}/${encodeURIComponent(JSON.stringify(stagedRecovery))}`);

    console.log('✅ Recovery applied to new conversion record');

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        recovery_id: recovery_id,
        applied: true,
        updated_fields: Object.keys(stagedRecovery.proposed_changes),
        conversion_key: conversionKey,
        message: 'Recovery successfully applied to new conversion record'
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
  if (attributionResult.method?.includes('lookup_key')) riskScore -= 1;
  
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

// Progress tracking functions for resume capability

// Check if a conversion has already been processed
async function checkIfAlreadyProcessed(email, timestamp) {
  try {
    const progressKey = `recovery_progress:${email}:${timestamp}`;
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData?.result) {
      const progress = JSON.parse(progressData.result);
      console.log(`📋 Found existing progress for ${email}: ${progress.recovery_id || 'no attribution found'}`);
      return progress;
    }
    
    return null;
  } catch (error) {
    console.log(`⚠️ Error checking progress for ${email}:`, error.message);
    return null;
  }
}

// Mark a conversion as processed (with or without attribution found)
async function markAsProcessed(email, timestamp, recoveryId, attributionFound) {
  try {
    const progressKey = `recovery_progress:${email}:${timestamp}`;
    const progressEntry = {
      email: email,
      timestamp: timestamp,
      processed_at: new Date().toISOString(),
      recovery_id: recoveryId,
      attribution_found: attributionFound,
      status: 'processed'
    };
    
    await redis(`set/${progressKey}/${encodeURIComponent(JSON.stringify(progressEntry))}`);
    console.log(`✅ Marked ${email} as processed (attribution: ${attributionFound ? 'found' : 'not found'})`);
    
    // Also update global progress counter
    await updateGlobalProgress(email, attributionFound);
    
  } catch (error) {
    console.log(`⚠️ Error marking ${email} as processed:`, error.message);
  }
}

// Update global progress statistics
async function updateGlobalProgress(email, attributionFound) {
  try {
    const globalProgressKey = 'recovery_global_progress';
    const progressData = await redis(`get/${globalProgressKey}`);
    
    let globalProgress = {
      total_processed: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: new Date().toISOString(),
      last_updated: new Date().toISOString()
    };
    
    if (progressData?.result) {
      globalProgress = JSON.parse(progressData.result);
    }
    
    globalProgress.total_processed++;
    globalProgress.last_updated = new Date().toISOString();
    globalProgress.last_processed_email = email;
    
    if (attributionFound) {
      globalProgress.attribution_found++;
    } else {
      globalProgress.no_attribution++;
    }
    
    await redis(`set/${globalProgressKey}/${encodeURIComponent(JSON.stringify(globalProgress))}`);
    
    // Log progress every 10 conversions
    if (globalProgress.total_processed % 10 === 0) {
      console.log(`📊 Global Progress: ${globalProgress.total_processed} processed, ${globalProgress.attribution_found} with attribution (${((globalProgress.attribution_found / globalProgress.total_processed) * 100).toFixed(1)}%)`);
    }
    
  } catch (error) {
    console.log('⚠️ Error updating global progress:', error.message);
  }
}

// Check if a conversion has already been RE-PROCESSED (first pass)
async function checkIfAlreadyReprocessed(email, timestamp) {
  try {
    const reprocessKey = `recovery_reprocess:${email}:${timestamp}`;
    const reprocessData = await redis(`get/${reprocessKey}`);
    
    if (reprocessData?.result) {
      const reprocess = JSON.parse(reprocessData.result);
      console.log(`📋 Found existing REPROCESS for ${email}: ${reprocess.recovery_id || 'no attribution found'}`);
      return reprocess;
    }
    
    return null;
  } catch (error) {
    console.log(`⚠️ Error checking reprocess status for ${email}:`, error.message);
    return null;
  }
}

// NEW: Check if a conversion has already been processed in SECOND REPROCESSING PASS
async function checkIfAlreadyReprocessed2(email, timestamp) {
  try {
    const reprocess2Key = `recovery_reprocess_2:${email}:${timestamp}`;
    const reprocess2Data = await redis(`get/${reprocess2Key}`);
    
    if (reprocess2Data?.result) {
      const reprocess2 = JSON.parse(reprocess2Data.result);
      console.log(`📋 Found existing SECOND REPROCESS for ${email}: ${reprocess2.recovery_id || 'no attribution found'}`);
      return reprocess2;
    }
    
    return null;
  } catch (error) {
    console.log(`⚠️ Error checking second reprocess status for ${email}:`, error.message);
    return null;
  }
}

// NEW: Mark a conversion as processed in SECOND REPROCESSING PASS
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
      reprocessing_pass: 'dual_ip_comprehensive_second_pass'
    };
    
    await redis(`set/${reprocess2Key}/${encodeURIComponent(JSON.stringify(reprocess2Entry))}`);
    console.log(`✅ Marked ${email} as SECOND REPROCESSED (attribution: ${attributionFound ? 'found' : 'not found'})`);
    
    // Also update second reprocessing progress counter
    await updateReprocessing2Progress(email, attributionFound);
    
  } catch (error) {
    console.log(`⚠️ Error marking ${email} as second reprocessed:`, error.message);
  }
}

// NEW: Update second reprocessing progress statistics
async function updateReprocessing2Progress(email, attributionFound) {
  try {
    const reprocess2ProgressKey = 'recovery_reprocessing_2_progress';
    const progressData = await redis(`get/${reprocess2ProgressKey}`);
    
    let reprocess2Progress = {
      total_reprocessed_2: 0,
      attribution_found: 0,
      no_attribution: 0,
      started_at: new Date().toISOString(),
      last_updated: new Date().toISOString(),
      reprocessing_reason: 'dual_ip_comprehensive_second_pass'
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
    
    await redis(`set/${reprocess2ProgressKey}/${encodeURIComponent(JSON.stringify(reprocess2Progress))}`);
    
    // Log progress every 10 conversions
    if (reprocess2Progress.total_reprocessed_2 % 10 === 0) {
      console.log(`🔄 SECOND REPROCESSING Progress: ${reprocess2Progress.total_reprocessed_2} reprocessed, ${reprocess2Progress.attribution_found} with attribution (${((reprocess2Progress.attribution_found / reprocess2Progress.total_reprocessed_2) * 100).toFixed(1)}%)`);
    }
    
  } catch (error) {
    console.log('⚠️ Error updating second reprocessing progress:', error.message);
  }
}

function encodeIPForKey(ip) {
  // Based on scanner results, attribution keys keep dots for IPv4
  // Only replace colons (IPv6) with underscores, keep dots for IPv4
  return ip.replace(/:/g, '_');
}
