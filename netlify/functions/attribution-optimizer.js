// 24-Hour Attribution Optimization System
// File: netlify/functions/attribution-optimizer.js

const handler = async (event, context) => {
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: corsHeaders, body: '' };
  }

  try {
    console.log('🚀 Starting 24-Hour Attribution Optimization System');
    
    const startTime = Date.now();
    const maxRunTime = 25000; // 25 seconds to avoid timeout
    
    // Initialize Redis
    const redis = await initializeRedis();
    
    // Step 1: Get all conversions that need optimization
    const allConversions = await getAllConversions(redis);
    console.log(`📊 Found ${allConversions.length} conversions to optimize`);
    
    // Step 2: Process conversions in batches
    const batchSize = 5; // Small batches to avoid timeout
    const results = [];
    let processedCount = 0;
    
    for (let i = 0; i < allConversions.length && (Date.now() - startTime) < maxRunTime; i += batchSize) {
      const batch = allConversions.slice(i, i + batchSize);
      console.log(`\n📦 Processing batch ${Math.floor(i/batchSize) + 1}: ${batch.length} conversions`);
      
      const batchResults = await processBatch(redis, batch);
      results.push(...batchResults);
      processedCount += batch.length;
      
      // Log progress
      const improvements = batchResults.filter(r => r.improved).length;
      console.log(`✅ Batch complete: ${improvements}/${batch.length} improved`);
    }
    
    const totalImprovements = results.filter(r => r.improved).length;
    const totalUpdated = results.filter(r => r.updated_in_redis).length;
    const executionTime = Date.now() - startTime;
    
    // Get optimization statistics
    const optimizationStats = await getOptimizationStats(redis);
    
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({
        success: true,
        summary: {
          total_conversions: allConversions.length,
          processed_conversions: processedCount,
          improvements_found: totalImprovements,
          records_updated: totalUpdated,
          improvement_rate: `${((totalImprovements / processedCount) * 100).toFixed(1)}%`,
          execution_time_ms: executionTime,
          total_historical_optimizations: optimizationStats.total_optimizations
        },
        results: results,
        optimization_stats: optimizationStats,
        next_batch_start: processedCount < allConversions.length ? processedCount : null
      })
    };

  } catch (error) {
    console.error('❌ Attribution optimization error:', error);
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

// Process a batch of conversions
async function processBatch(redis, conversions) {
  const batchResults = [];
  
  for (const conversion of conversions) {
    console.log(`\n🔍 Optimizing: ${conversion.email}`);
    
    try {
      const optimizationResult = await optimizeConversionAttribution(redis, conversion);
      batchResults.push(optimizationResult);
      
      // Log result
      if (optimizationResult.improved) {
        console.log(`✅ IMPROVED: ${optimizationResult.improvement_type}`);
        console.log(`   Old: ${optimizationResult.old_attribution?.method || 'none'} (${optimizationResult.old_attribution?.score || 0})`);
        console.log(`   New: ${optimizationResult.new_attribution.method} (${optimizationResult.new_attribution.score})`);
      } else {
        console.log(`✓ Optimal: ${optimizationResult.current_attribution?.method || 'none'}`);
      }
      
    } catch (error) {
      console.error(`❌ Failed to optimize ${conversion.email}:`, error);
      batchResults.push({
        email: conversion.email,
        improved: false,
        error: error.message
      });
    }
  }
  
  return batchResults;
}

// Core optimization logic for a single conversion
async function optimizeConversionAttribution(redis, conversion) {
  const conversionTime = new Date(conversion.timestamp);
  const windowStart = new Date(conversionTime.getTime() - (24 * 60 * 60 * 1000));
  
  console.log(`   📅 24-hour window: ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
  
  // Extract both IPs from conversion data
  const extractedIPs = extractConversionIPs(conversion);
  console.log(`   📍 IPs to check: ${extractedIPs.join(', ')}`);
  
  // Get pageviews within 24-hour window
  const relevantPageviews = await getPageviewsInWindow(redis, windowStart, conversionTime);
  console.log(`   📄 Found ${relevantPageviews.length} pageviews in window`);
  
  // Find best attribution match using both IPs
  const bestMatch = await findBestAttributionMatch(extractedIPs, relevantPageviews, conversionTime);
  
  // Compare with existing attribution
  const currentAttribution = getCurrentAttribution(conversion);
  const shouldUpdate = shouldUpdateAttribution(currentAttribution, bestMatch);
  
  const result = {
    email: conversion.email,
    conversion_timestamp: conversion.timestamp,
    extracted_ips: extractedIPs,
    pageviews_in_window: relevantPageviews.length,
    current_attribution: currentAttribution,
    best_match_found: bestMatch,
    improved: shouldUpdate,
    improvement_type: shouldUpdate ? getImprovementType(currentAttribution, bestMatch) : null,
    old_attribution: shouldUpdate ? currentAttribution : null,
    new_attribution: shouldUpdate ? bestMatch : null
  };

  // If improvement found, update the conversion record
  if (shouldUpdate && bestMatch) {
    await updateConversionRecord(redis, conversion, bestMatch, currentAttribution);
    result.updated_in_redis = true;
    console.log(`   💾 Updated conversion record in Redis`);
  }

  return result;
}

// Extract both pageview and conversion IPs from webhook data
function extractConversionIPs(conversion) {
  const ips = [];
  
  // Primary IP (usually pageview IP)
  if (conversion.ip_address) {
    ips.push(conversion.ip_address);
  }
  
  // Additional IPs from dual IP extraction
  if (conversion.pageview_ip && conversion.pageview_ip !== conversion.ip_address) {
    ips.push(conversion.pageview_ip);
  }
  
  if (conversion.conversion_ip && !ips.includes(conversion.conversion_ip)) {
    ips.push(conversion.conversion_ip);
  }
  
  // Check for unique IPs array (from enhanced extraction)
  if (conversion.unique_ips && Array.isArray(conversion.unique_ips)) {
    conversion.unique_ips.forEach(ip => {
      if (!ips.includes(ip)) {
        ips.push(ip);
      }
    });
  }
  
  return ips.filter(ip => ip && ip !== 'unknown');
}

// Get pageviews within 24-hour window before conversion
async function getPageviewsInWindow(redis, windowStart, conversionTime) {
  console.log(`     🔍 Scanning pageviews from ${windowStart.toISOString()} to ${conversionTime.toISOString()}`);
  
  try {
    // Get all attribution keys (pageviews) using comprehensive scanning
    const attributionKeys = await getComprehensiveAttributionKeys(redis);
    
    // Filter pageviews to 24-hour window
    const relevantPageviews = [];
    const batchSize = 100;
    
    for (let i = 0; i < attributionKeys.length; i += batchSize) {
      const batch = attributionKeys.slice(i, i + batchSize);
      
      const batchPageviews = await Promise.all(
        batch.map(async (key) => {
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
      
      relevantPageviews.push(...batchPageviews.filter(pv => pv !== null));
      
      // Small delay to avoid overwhelming Redis
      if (i + batchSize < attributionKeys.length) {
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    }
    
    return relevantPageviews;
    
  } catch (error) {
    console.error('❌ Failed to get pageviews in window:', error);
    return [];
  }
}

// Find best attribution match from available pageviews
async function findBestAttributionMatch(conversionIPs, pageviews, conversionTime) {
  let bestMatch = null;
  let highestScore = 0;
  
  console.log(`     🎯 Checking ${conversionIPs.length} IPs against ${pageviews.length} pageviews`);
  
  for (const ip of conversionIPs) {
    for (const pageview of pageviews) {
      if (pageview.ip_address === ip) {
        // Calculate attribution score based on method and timing
        const timeDiff = new Date(conversionTime) - new Date(pageview.timestamp);
        const score = calculateAttributionScore('direct_ip_match', timeDiff);
        
        console.log(`       ✅ IP match found: ${ip} -> ${pageview.landing_page} (${score} pts)`);
        
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

// Attribution scoring system
function calculateAttributionScore(method, timeDiffMs) {
  const baseScores = {
    'direct_ip_match': 280,
    'session_match': 300,
    'device_match': 220,
    'geo_correlation': 80
  };
  
  let baseScore = baseScores[method] || 0;
  
  // Time decay: reduce score based on time between pageview and conversion
  const hours = timeDiffMs / (1000 * 60 * 60);
  if (hours <= 1) {
    // No decay for conversions within 1 hour
    return baseScore;
  } else if (hours <= 6) {
    // 10% decay for 1-6 hours
    return Math.round(baseScore * 0.9);
  } else if (hours <= 24) {
    // 20% decay for 6-24 hours
    return Math.round(baseScore * 0.8);
  } else {
    // Should not happen in 24-hour window, but handle gracefully
    return Math.round(baseScore * 0.5);
  }
}

// Get current attribution from conversion record
function getCurrentAttribution(conversion) {
  if (!conversion.attribution_found) {
    return {
      method: 'none',
      score: 0,
      landing_page: null,
      source: null
    };
  }
  
  return {
    method: conversion.attribution_method,
    score: conversion.attribution_score || 0,
    landing_page: conversion.landing_page,
    source: conversion.source,
    utm_campaign: conversion.campaign
  };
}

// Determine if new attribution is better than current
function shouldUpdateAttribution(current, newMatch) {
  if (!newMatch) return false;
  if (!current || current.method === 'none') return true;
  
  // Update if new match has higher score
  return newMatch.score > current.score;
}

// Determine type of improvement
function getImprovementType(oldAttribution, newAttribution) {
  if (!oldAttribution || oldAttribution.method === 'none') {
    return 'NEW_ATTRIBUTION';
  }
  
  if (newAttribution.score > oldAttribution.score) {
    return 'HIGHER_CONFIDENCE';
  }
  
  return 'SAME_CONFIDENCE';
}

// Update conversion record with optimized attribution
async function updateConversionRecord(redis, conversion, newAttribution, oldAttribution) {
  try {
    const conversionKey = conversion._redis_key;
    if (!conversionKey) {
      throw new Error('No Redis key found for conversion');
    }

    console.log(`     💾 Updating conversion record: ${conversionKey}`);

    // Create updated conversion record
    const updatedConversion = {
      ...conversion,
      
      // Update primary attribution fields
      attribution_found: true,
      attribution_method: newAttribution.method,
      attribution_score: newAttribution.score,
      landing_page: newAttribution.landing_page,
      source: newAttribution.source,
      campaign: newAttribution.utm_campaign || conversion.campaign,
      medium: newAttribution.utm_medium || conversion.medium,
      
      // Add optimization metadata
      attribution_optimized: true,
      optimization_timestamp: new Date().toISOString(),
      optimization_version: "24h_dual_ip_v1.0",
      optimization_matched_ip: newAttribution.matched_ip,
      optimization_time_diff_minutes: newAttribution.time_difference_minutes,
      
      // Preserve original attribution for audit trail
      original_attribution: {
        method: oldAttribution?.method || 'none',
        score: oldAttribution?.score || 0,
        landing_page: oldAttribution?.landing_page || null,
        source: oldAttribution?.source || null,
        preserved_at: new Date().toISOString()
      }
    };

    // Store updated conversion record
    await redis(`set/${conversionKey}/${encodeURIComponent(JSON.stringify(updatedConversion))}`);
    
    // Also create optimization log entry
    await createOptimizationLogEntry(redis, conversion, newAttribution, oldAttribution);
    
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
      optimization_version: "24h_dual_ip_v1.0",
      
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

// Get optimization statistics
async function getOptimizationStats(redis) {
  try {
    console.log('📊 Gathering optimization statistics...');
    
    // Scan for optimization log entries
    let cursor = '0';
    let optimizationKeys = [];
    
    do {
      const result = await redis(`scan/${cursor}/match/optimization:*/count/1000`);
      if (result.result && result.result[1]) {
        cursor = result.result[0];
        optimizationKeys = optimizationKeys.concat(result.result[1]);
      } else {
        break;
      }
    } while (cursor !== '0' && optimizationKeys.length < 5000);
    
    console.log(`📋 Found ${optimizationKeys.length} optimization records`);
    
    return {
      total_optimizations: optimizationKeys.length,
      optimization_keys: optimizationKeys.slice(0, 100) // Return sample for debugging
    };
    
  } catch (error) {
    console.error('❌ Failed to get optimization stats:', error);
    return { total_optimizations: 0, error: error.message };
  }
}

// Utility functions
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

async function getAllConversions(redis) {
  // Use analytics-flexible.js method to get all conversions
  const response = await fetch('https://trackingojoy.netlify.app/.netlify/functions/analytics-flexible?start_date=2025-07-01&end_date=2025-07-02');
  const result = await response.json();
  return result.conversions || [];
}

// Comprehensive attribution key scanning (from analytics-flexible.js)
async function getComprehensiveAttributionKeys(redis) {
  let allAttributionKeys = [];
  
  try {
    let cursor = '0';
    
    do {
      const result = await redis(`scan/${cursor}/match/attribution_*/count/1000`);
      if (result.result && result.result[1]) {
        cursor = result.result[0];
        const keys = result.result[1];
        allAttributionKeys = allAttributionKeys.concat(keys);
      } else {
        break;
      }
    } while (cursor !== '0' && allAttributionKeys.length < 15000);
    
    return allAttributionKeys;
    
  } catch (error) {
    console.error('❌ Attribution key scanning failed:', error);
    return [];
  }
}

module.exports = { handler };
