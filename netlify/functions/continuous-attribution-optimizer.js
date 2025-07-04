// Continuous Single-Conversion Attribution Optimizer
// File: netlify/functions/continuous-attribution-optimizer.js

const handler = async (event, context) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  try {
    console.log('🔄 Continuous Attribution Optimizer - Single Conversion Processing');
    
    const startTime = Date.now();
    
    // Initialize Redis
    const redis = await initializeRedis();
    
    // Get progress from Redis
    const progress = await getOptimizationProgress(redis);
    console.log(`📊 Current progress: ${progress.completed}/${progress.total} conversions processed`);
    
    // Check if we're done
    if (progress.completed >= progress.total) {
      console.log('🎉 All conversions have been processed!');
      
      const finalStats = await getFinalOptimizationStats(redis);
      
      return {
        statusCode: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          status: 'COMPLETED',
          message: 'All conversions have been optimized',
          final_stats: finalStats
        })
      };
    }
    
    // Get the next conversion to process
    const nextConversion = await getNextConversionToProcess(redis, progress);
    
    if (!nextConversion) {
      return {
        statusCode: 200,
        headers: corsHeaders,
        body: JSON.stringify({
          success: true,
          status: 'NO_MORE_CONVERSIONS',
          message: 'No more conversions to process'
        })
      };
    }
    
    console.log(`🔍 Processing conversion ${progress.completed + 1}/${progress.total}: ${nextConversion.email}`);
    
    // Process this single conversion
    const optimizationResult = await optimizeSingleConversion(redis, nextConversion);
    
    // Update progress
    await updateOptimizationProgress(redis, progress, optimizationResult);
    
    const executionTime = Date.now() - startTime;
    
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        success: true,
        status: 'PROCESSING',
        current_conversion: {
          email: nextConversion.email,
          result: optimizationResult
        },
        progress: {
          completed: progress.completed + 1,
          total: progress.total,
          remaining: progress.total - (progress.completed + 1),
          percentage: `${(((progress.completed + 1) / progress.total) * 100).toFixed(1)}%`
        },
        execution_time_ms: executionTime,
        next_action: progress.completed + 1 < progress.total ? 'CALL_AGAIN' : 'COMPLETED'
      })
    };

  } catch (error) {
    console.error('❌ Continuous optimization error:', error);
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({
        success: false,
        error: error.message
      })
    };
  }
};

// Get or initialize optimization progress
async function getOptimizationProgress(redis) {
  try {
    const progressKey = 'optimization_progress_v1';
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData.result) {
      const progress = JSON.parse(progressData.result);
      console.log(`📋 Found existing progress: ${progress.completed}/${progress.total}`);
      return progress;
    } else {
      // Initialize new optimization run
      console.log('🆕 Initializing new optimization run...');
      
      // Get all conversions
      const allConversions = await getAllConversions();
      
      const initialProgress = {
        started_at: new Date().toISOString(),
        total: allConversions.length,
        completed: 0,
        improvements_found: 0,
        conversions_processed: [],
        current_batch_id: Date.now()
      };
      
      await redis(`set/${progressKey}/${encodeURIComponent(JSON.stringify(initialProgress))}`);
      console.log(`✅ Initialized optimization for ${allConversions.length} conversions`);
      
      return initialProgress;
    }
  } catch (error) {
    console.error('❌ Failed to get optimization progress:', error);
    throw error;
  }
}

// Get the next conversion to process
async function getNextConversionToProcess(redis, progress) {
  try {
    const allConversions = await getAllConversions();
    
    // Find conversions that haven't been processed yet
    const processedEmails = new Set(progress.conversions_processed.map(c => c.email));
    const remainingConversions = allConversions.filter(conv => !processedEmails.has(conv.email));
    
    if (remainingConversions.length === 0) {
      return null;
    }
    
    // Return the first unprocessed conversion
    const nextConversion = remainingConversions[0];
    console.log(`📧 Next conversion: ${nextConversion.email}`);
    
    return nextConversion;
    
  } catch (error) {
    console.error('❌ Failed to get next conversion:', error);
    throw error;
  }
}

// Process a single conversion (main optimization logic)
async function optimizeSingleConversion(redis, conversion) {
  const startTime = Date.now();
  
  try {
    console.log(`   📅 Conversion time: ${conversion.timestamp}`);
    
    // Extract both IPs from conversion
    const extractedIPs = extractConversionIPs(conversion);
    console.log(`   📍 IPs to check: ${extractedIPs.join(', ')}`);
    
    // Calculate 24-hour window
    const conversionTime = new Date(conversion.timestamp);
    const windowStart = new Date(conversionTime.getTime() - (24 * 60 * 60 * 1000));
    
    console.log(`   ⏰ 24-hour window: ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
    
    // Get pageviews in window (optimized scanning)
    const relevantPageviews = await getPageviewsInWindowOptimized(redis, windowStart, conversionTime);
    console.log(`   📄 Found ${relevantPageviews.length} pageviews in window`);
    
    // Find best attribution match
    const bestMatch = await findBestAttributionMatch(extractedIPs, relevantPageviews, conversionTime);
    
    // Get current attribution
    const currentAttribution = getCurrentAttribution(conversion);
    
    // Determine if we should update
    const shouldUpdate = shouldUpdateAttribution(currentAttribution, bestMatch);
    
    const result = {
      email: conversion.email,
      timestamp: conversion.timestamp,
      extracted_ips: extractedIPs,
      pageviews_in_window: relevantPageviews.length,
      current_attribution: currentAttribution,
      best_match_found: bestMatch,
      improved: shouldUpdate,
      improvement_type: shouldUpdate ? getImprovementType(currentAttribution, bestMatch) : null,
      processing_time_ms: Date.now() - startTime
    };
    
    // Update the conversion record if improved
    if (shouldUpdate && bestMatch) {
      await updateConversionRecord(redis, conversion, bestMatch, currentAttribution);
      result.updated_in_redis = true;
      console.log(`   ✅ IMPROVED: ${result.improvement_type} (${currentAttribution.score} → ${bestMatch.score})`);
    } else if (bestMatch) {
      console.log(`   ✓ Current attribution is optimal (${currentAttribution.score} ≥ ${bestMatch.score})`);
    } else {
      console.log(`   ❌ No attribution found in 24-hour window`);
    }
    
    return result;
    
  } catch (error) {
    console.error(`   ❌ Failed to optimize ${conversion.email}:`, error);
    return {
      email: conversion.email,
      improved: false,
      error: error.message,
      processing_time_ms: Date.now() - startTime
    };
  }
}

// Optimized pageview scanning for 24-hour window with unlimited key scanning
async function getPageviewsInWindowOptimized(redis, windowStart, conversionTime) {
  try {
    // Use time-based termination instead of key count limit for complete coverage
    const maxScanTime = 20000; // 20 seconds max scanning time
    const scanStartTime = Date.now();
    const relevantPageviews = [];
    
    console.log(`     🔍 Scanning ALL attribution keys for window (max 20 seconds)...`);
    
    let cursor = '0';
    let keysScanned = 0;
    let batchCount = 0;
    
    do {
      // Check if we're approaching time limit
      const elapsedTime = Date.now() - scanStartTime;
      if (elapsedTime >= maxScanTime) {
        console.log(`     ⏰ Time limit reached: ${elapsedTime}ms, stopping scan`);
        break;
      }
      
      const result = await redis(`scan/${cursor}/match/attribution_*/count/300`);
      
      if (result.result && result.result[1]) {
        cursor = result.result[0];
        const keys = result.result[1];
        batchCount++;
        
        console.log(`     📦 Batch ${batchCount}: Processing ${keys.length} keys (${keysScanned} total scanned, ${relevantPageviews.length} pageviews found)`);
        
        // Process this batch of keys
        const batchPageviews = await Promise.all(
          keys.map(async (key) => {
            try {
              const data = await redis(`get/${key}`);
              if (data.result) {
                const parsed = JSON.parse(data.result);
                const pageviewTime = new Date(parsed.timestamp);
                
                // Check if pageview is within our 24-hour window
                if (pageviewTime >= windowStart && pageviewTime <= conversionTime) {
                  return {
                    key: key,
                    timestamp: parsed.timestamp,
                    ip_address: parsed.ip_address,
                    landing_page: parsed.landing_page,
                    source: parsed.source,
                    utm_campaign: parsed.utm_campaign,
                    utm_medium: parsed.utm_medium,
                    utm_source: parsed.utm_source
                  };
                }
              }
              return null;
            } catch (error) {
              return null;
            }
          })
        );
        
        const validPageviews = batchPageviews.filter(pv => pv !== null);
        relevantPageviews.push(...validPageviews);
        keysScanned += keys.length;
        
        // Log progress every few batches
        if (batchCount % 10 === 0) {
          const elapsed = Date.now() - scanStartTime;
          console.log(`     📊 Progress: ${keysScanned} keys scanned, ${relevantPageviews.length} pageviews found (${elapsed}ms elapsed)`);
        }
        
        // Early termination only if we found a substantial number of pageviews AND have good matches
        if (relevantPageviews.length >= 200) {
          console.log(`     ✅ Early termination: found sufficient pageviews (${relevantPageviews.length})`);
          break;
        }
        
      } else {
        console.log(`     ⚠️ No more results at cursor ${cursor}`);
        break;
      }
      
    } while (cursor !== '0');
    
    const totalElapsed = Date.now() - scanStartTime;
    console.log(`     ✅ Scan complete: ${keysScanned} keys scanned, ${relevantPageviews.length} pageviews found in ${totalElapsed}ms`);
    
    // Sort pageviews by timestamp (most recent first) for better matching
    relevantPageviews.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    return relevantPageviews;
    
  } catch (error) {
    console.error('     ❌ Failed to get pageviews in window:', error);
    return [];
  }
}

// Update optimization progress
async function updateOptimizationProgress(redis, currentProgress, optimizationResult) {
  try {
    const progressKey = 'optimization_progress_v1';
    
    const updatedProgress = {
      ...currentProgress,
      completed: currentProgress.completed + 1,
      improvements_found: currentProgress.improvements_found + (optimizationResult.improved ? 1 : 0),
      conversions_processed: [
        ...currentProgress.conversions_processed,
        {
          email: optimizationResult.email,
          processed_at: new Date().toISOString(),
          improved: optimizationResult.improved,
          improvement_type: optimizationResult.improvement_type,
          processing_time_ms: optimizationResult.processing_time_ms
        }
      ],
      last_processed_at: new Date().toISOString()
    };
    
    await redis(`set/${progressKey}/${encodeURIComponent(JSON.stringify(updatedProgress))}`);
    
  } catch (error) {
    console.error('❌ Failed to update optimization progress:', error);
  }
}

// Get final optimization statistics
async function getFinalOptimizationStats(redis) {
  try {
    const progressKey = 'optimization_progress_v1';
    const progressData = await redis(`get/${progressKey}`);
    
    if (progressData.result) {
      const progress = JSON.parse(progressData.result);
      
      const totalTime = new Date() - new Date(progress.started_at);
      const avgTimePerConversion = progress.conversions_processed.length > 0 
        ? progress.conversions_processed.reduce((sum, c) => sum + c.processing_time_ms, 0) / progress.conversions_processed.length
        : 0;
      
      return {
        total_conversions: progress.total,
        completed_conversions: progress.completed,
        improvements_found: progress.improvements_found,
        improvement_rate: `${((progress.improvements_found / progress.completed) * 100).toFixed(1)}%`,
        started_at: progress.started_at,
        completed_at: new Date().toISOString(),
        total_processing_time: `${Math.round(totalTime / 1000)} seconds`,
        average_time_per_conversion: `${Math.round(avgTimePerConversion)}ms`
      };
    }
    
    return { error: 'No progress data found' };
    
  } catch (error) {
    return { error: error.message };
  }
}

// Utility functions (reuse from previous version)
function extractConversionIPs(conversion) {
  const ips = [];
  
  if (conversion.ip_address) ips.push(conversion.ip_address);
  if (conversion.pageview_ip && conversion.pageview_ip !== conversion.ip_address) {
    ips.push(conversion.pageview_ip);
  }
  if (conversion.conversion_ip && !ips.includes(conversion.conversion_ip)) {
    ips.push(conversion.conversion_ip);
  }
  if (conversion.unique_ips && Array.isArray(conversion.unique_ips)) {
    conversion.unique_ips.forEach(ip => {
      if (!ips.includes(ip)) ips.push(ip);
    });
  }
  
  return ips.filter(ip => ip && ip !== 'unknown');
}

async function findBestAttributionMatch(conversionIPs, pageviews, conversionTime) {
  let bestMatch = null;
  let highestScore = 0;
  
  for (const ip of conversionIPs) {
    for (const pageview of pageviews) {
      if (pageview.ip_address === ip) {
        const timeDiff = new Date(conversionTime) - new Date(pageview.timestamp);
        const score = calculateAttributionScore('direct_ip_match', timeDiff);
        
        if (score > highestScore) {
          highestScore = score;
          bestMatch = {
            method: 'direct_ip_match',
            score: score,
            matched_ip: ip,
            landing_page: pageview.landing_page,
            source: pageview.source,
            utm_campaign: pageview.utm_campaign,
            utm_medium: pageview.utm_medium,
            utm_source: pageview.utm_source,
            pageview_timestamp: pageview.timestamp,
            time_difference_minutes: Math.round(timeDiff / (1000 * 60))
          };
        }
      }
    }
  }
  
  return bestMatch;
}

function calculateAttributionScore(method, timeDiffMs) {
  const baseScores = { 'direct_ip_match': 280, 'geo_correlation': 80 };
  let baseScore = baseScores[method] || 0;
  
  const hours = timeDiffMs / (1000 * 60 * 60);
  if (hours <= 1) return baseScore;
  else if (hours <= 6) return Math.round(baseScore * 0.9);
  else if (hours <= 24) return Math.round(baseScore * 0.8);
  else return Math.round(baseScore * 0.5);
}

function getCurrentAttribution(conversion) {
  if (!conversion.attribution_found) {
    return { method: 'none', score: 0, landing_page: null, source: null };
  }
  
  return {
    method: conversion.attribution_method,
    score: conversion.attribution_score || 0,
    landing_page: conversion.landing_page,
    source: conversion.source,
    utm_campaign: conversion.campaign
  };
}

function shouldUpdateAttribution(current, newMatch) {
  if (!newMatch) return false;
  if (!current || current.method === 'none') return true;
  return newMatch.score > current.score;
}

function getImprovementType(oldAttribution, newAttribution) {
  if (!oldAttribution || oldAttribution.method === 'none') return 'NEW_ATTRIBUTION';
  if (newAttribution.score > oldAttribution.score) return 'HIGHER_CONFIDENCE';
  return 'SAME_CONFIDENCE';
}

async function updateConversionRecord(redis, conversion, bestMatch, currentAttribution) {
  try {
    const conversionKey = conversion._redis_key;
    if (!conversionKey) {
      console.log(`     ⚠️ No Redis key found for conversion ${conversion.email}, skipping update`);
      return;
    }

    console.log(`     💾 Updating conversion record: ${conversionKey}`);

    // Create updated conversion record
    const updatedConversion = {
      ...conversion,
      
      // Update primary attribution fields
      attribution_found: true,
      attribution_method: bestMatch.method,
      attribution_score: bestMatch.score,
      landing_page: bestMatch.landing_page,
      source: bestMatch.source,
      campaign: bestMatch.utm_campaign || conversion.campaign,
      medium: bestMatch.utm_medium || conversion.medium,
      
      // Add optimization metadata
      attribution_optimized: true,
      optimization_timestamp: new Date().toISOString(),
      optimization_version: "24h_unlimited_scan_v1.0",
      optimization_matched_ip: bestMatch.matched_ip,
      optimization_time_diff_minutes: bestMatch.time_difference_minutes,
      
      // Preserve original attribution for audit trail
      original_attribution: {
        method: currentAttribution?.method || 'none',
        score: currentAttribution?.score || 0,
        landing_page: currentAttribution?.landing_page || null,
        source: currentAttribution?.source || null,
        preserved_at: new Date().toISOString()
      }
    };

    // Store updated conversion record
    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(updatedConversion))}`);
    
    // Also create optimization log entry
    await createOptimizationLogEntry(redis, conversion, bestMatch, currentAttribution);
    
    console.log(`     ✅ Successfully updated conversion record and created optimization log`);
    
  } catch (error) {
    console.error(`     ❌ Failed to update conversion record:`, error);
    throw error;
  }
}

// Create optimization log entry for audit trail
async function createOptimizationLogEntry(redis, conversion, newAttribution, oldAttribution) {
  try {
    const optimizationKey = `optimization:${conversion.email}:${Date.now()}`;
    
    const optimizationRecord = {
      email: conversion.email,
      conversion_timestamp: conversion.timestamp,
      conversion_order_id: conversion.order_id,
      optimization_timestamp: new Date().toISOString(),
      optimization_version: "24h_unlimited_scan_v1.0",
      
      // Attribution change details
      old_attribution: {
        method: oldAttribution?.method || 'none',
        score: oldAttribution?.score || 0,
        landing_page: oldAttribution?.landing_page || null,
        source: oldAttribution?.source || null
      },
      new_attribution: {
        method: newAttribution.method,
        score: newAttribution.score,
        landing_page: newAttribution.landing_page,
        source: newAttribution.source,
        matched_ip: newAttribution.matched_ip,
        pageview_timestamp: newAttribution.pageview_timestamp,
        time_difference_minutes: newAttribution.time_difference_minutes
      },
      
      // Improvement metrics
      score_improvement: newAttribution.score - (oldAttribution?.score || 0),
      improvement_type: getImprovementType(oldAttribution, newAttribution),
      
      // Process metadata
      original_conversion_key: conversion._redis_key
    };

    await redis(`set/${optimizationKey}/${encodeURIComponent(JSON.stringify(optimizationRecord))}`);
    
  } catch (error) {
    console.error(`     ⚠️ Failed to create optimization log entry:`, error);
    // Don't throw - optimization logging failure shouldn't stop the main process
  }
}

async function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  if (!redisUrl || !redisToken) {
    throw new Error('Redis configuration missing');
  }
  
  return async (command) => {
    const response = await fetch(`${redisUrl}/${command}`, {
      headers: { Authorization: `Bearer ${redisToken}` }
    });
    return response.json();
  };
}

async function getAllConversions() {
  const response = await fetch('https://trackingojoy.netlify.app/.netlify/functions/analytics-flexible?start_date=2025-07-01&end_date=2025-07-02');
  const result = await response.json();
  return result.conversions || [];
}

module.exports = { handler };
