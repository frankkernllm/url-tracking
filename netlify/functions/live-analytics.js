// live-analytics.js - Real-Time Conversion Analytics from Track.js Data
// Path: netlify/functions/live-analytics.js  
// Purpose: Ultra-fast real-time analytics using direct conversions:* data from track.js

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key, Authorization',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Max-Age': '86400',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  // Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  // Validate API key
  const apiKey = event.headers['x-api-key'] || event.headers['X-API-Key'];
  if (apiKey !== process.env.OJOY_API_KEY) {
    return {
      statusCode: 401,
      headers,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  try {
    console.log('⚡ LIVE-ANALYTICS: Starting real-time conversion analytics from track.js data');
    const startTime = Date.now();
    
    // Parse query parameters with validation
    const queryParams = event.queryStringParameters || {};
    const startDate = validateDate(queryParams.start_date) || getDateDaysAgo(7);
    const endDate = validateDate(queryParams.end_date) || getDateDaysAgo(0);
    const limit = validateLimit(queryParams.limit) || 1000;
    const includeToday = queryParams.include_today !== 'false'; // Default true
    
    console.log(`📅 Date range: ${startDate} to ${endDate} (limit: ${limit}, include_today: ${includeToday})`);
    
    const redis = initializeRedis();
    
    // STEP 1: Get real-time conversions directly from track.js data (FAST)
    console.log('💰 Fetching real-time conversions from track.js storage...');
    const conversions = await getConversionsFromTrackJS(redis, startDate, endDate, limit, includeToday);
    console.log(`✅ Found ${conversions.length} real-time conversions`);
    
    // STEP 2: Get attribution statistics for enhanced analysis (FAST)
    console.log('📊 Fetching attribution statistics...');
    const attributionStats = await getAttributionStats(redis, startDate, endDate);
    console.log(`✅ Found ${attributionStats.length} attribution stat records`);
    
    // STEP 3: Perform real-time attribution analysis (FAST)
    console.log('🔗 Performing real-time attribution analysis...');
    const attributionAnalysis = performRealTimeAttribution(conversions, attributionStats);
    
    // STEP 4: Generate live analytics summary (FAST)
    const liveAnalytics = generateLiveAnalyticsSummary(conversions, attributionAnalysis);
    
    const totalTime = Date.now() - startTime;
    console.log(`⚡ Live analytics complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        // Real-time conversion data
        conversions: conversions,
        
        // Live attribution analysis
        attribution_analysis: attributionAnalysis,
        
        // Real-time summary metrics
        total_conversions: conversions.length,
        total_revenue: liveAnalytics.total_revenue.toFixed(2),
        attribution_success_rate: liveAnalytics.attribution_success_rate,
        unique_customers: liveAnalytics.unique_customers,
        
        // Live attribution breakdown
        attribution_methods: attributionAnalysis.methods_breakdown,
        confidence_distribution: attributionAnalysis.confidence_distribution,
        dual_ip_analysis: attributionAnalysis.dual_ip_analysis,
        
        // Source performance (real-time)
        source_performance: attributionAnalysis.source_performance,
        
        // Date range info
        date_range: {
          start: startDate,
          end: endDate,
          days: getDaysBetween(startDate, endDate),
          include_today: includeToday,
          data_source: 'track_js_direct'
        },
        
        // Performance metrics
        processing_stats: {
          execution_time_ms: totalTime,
          data_source: 'conversions_keys_direct',
          conversion_keys_scanned: liveAnalytics.keys_scanned,
          attribution_stats_loaded: attributionStats.length,
          real_time_data: true,
          data_freshness: 'real_time'
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Live analytics error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Live analytics failed', 
        message: error.message 
      })
    };
  }
};

// Get real-time conversions directly from track.js storage
async function getConversionsFromTrackJS(redis, startDate, endDate, limit, includeToday) {
  console.log('💰 Loading real-time conversions from conversions:* keys...');
  
  const conversions = [];
  let keysScanned = 0;
  
  try {
    // Generate date patterns for efficient scanning
    const datePatterns = generateDatePatterns(startDate, endDate, includeToday);
    console.log(`🔍 Scanning ${datePatterns.length} date patterns:`, datePatterns.slice(0, 3));
    
    // Scan each date pattern
    for (const pattern of datePatterns) {
      if (conversions.length >= limit) break;
      
      let cursor = '0';
      let iterations = 0;
      const maxIterations = 10; // Limit per date pattern
      
      do {
        const scanResult = await redis(`scan/${cursor}/match/${pattern}/count/200`, 1500);
        
        if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
          break;
        }
        
        cursor = scanResult.result[0];
        const keys = scanResult.result[1] || [];
        keysScanned += keys.length;
        iterations++;
        
        // Load conversions from this batch
        const batchSize = 20;
        for (let i = 0; i < keys.length; i += batchSize) {
          if (conversions.length >= limit) break;
          
          const batch = keys.slice(i, i + batchSize);
          
          const batchPromises = batch.map(async (key) => {
            try {
              const conversionData = await redis(`get/${key}`, 1000);
              if (conversionData?.result) {
                const conversion = JSON.parse(decodeURIComponent(conversionData.result));
                
                // Validate and filter conversion
                if (isValidConversion(conversion) && 
                    isWithinDateRange(conversion.timestamp, startDate, endDate)) {
                  
                  return extractConversionData(conversion, key);
                }
              }
            } catch (parseError) {
              console.warn(`⚠️ Failed to parse conversion ${key}`);
            }
            return null;
          });
          
          const batchResults = await Promise.all(batchPromises);
          const validConversions = batchResults.filter(conv => conv !== null);
          conversions.push(...validConversions);
        }
        
      } while (cursor !== '0' && iterations < maxIterations && conversions.length < limit);
    }
    
    // Sort by timestamp (most recent first)
    conversions.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    
    console.log(`✅ Loaded ${conversions.length} real-time conversions from ${keysScanned} keys scanned`);
    
    // Store scan count for reporting
    conversions._keysScanned = keysScanned;
    
    return conversions;
    
  } catch (error) {
    console.error('❌ Real-time conversion loading failed:', error);
    return [];
  }
}

// Get attribution statistics for enhanced analysis
async function getAttributionStats(redis, startDate, endDate) {
  console.log('📊 Loading attribution statistics...');
  
  const attributionStats = [];
  
  try {
    // Scan for attribution_stats_* keys
    let cursor = '0';
    let iterations = 0;
    const maxIterations = 5; // Limit for stats
    
    do {
      const scanResult = await redis(`scan/${cursor}/match/attribution_stats_*/count/100`, 1500);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Load stats in parallel
      const batchPromises = keys.slice(0, 50).map(async (key) => { // Limit to 50 stats
        try {
          const statsData = await redis(`get/${key}`, 800);
          if (statsData?.result) {
            const stats = JSON.parse(decodeURIComponent(statsData.result));
            
            if (isWithinDateRange(stats.timestamp, startDate, endDate)) {
              return {
                timestamp: stats.timestamp,
                method: stats.method,
                score: stats.score,
                success: stats.success,
                dual_ip: stats.dual_ip,
                fields_available: stats.fields_available
              };
            }
          }
        } catch (parseError) {
          // Skip invalid stats
        }
        return null;
      });
      
      const batchResults = await Promise.all(batchPromises);
      const validStats = batchResults.filter(stat => stat !== null);
      attributionStats.push(...validStats);
      
    } while (cursor !== '0' && iterations < maxIterations);
    
    console.log(`✅ Loaded ${attributionStats.length} attribution stat records`);
    return attributionStats;
    
  } catch (error) {
    console.warn('⚠️ Attribution stats loading failed:', error);
    return [];
  }
}

// Perform real-time attribution analysis
function performRealTimeAttribution(conversions, attributionStats) {
  const analysisStart = Date.now();
  console.log('🔗 Performing real-time attribution analysis...');
  
  const analysis = {
    total_conversions: conversions.length,
    attributed_conversions: 0,
    unattributed_conversions: 0,
    methods_breakdown: {},
    confidence_distribution: { high: 0, medium: 0, low: 0 },
    dual_ip_analysis: { total: 0, percentage: 0 },
    source_performance: {},
    ip_extraction_analysis: { primary_ip: 0, conversion_ip: 0, pageview_ip: 0 }
  };
  
  // Analyze each conversion
  for (const conversion of conversions) {
    // Attribution success tracking
    if (conversion.attribution_found) {
      analysis.attributed_conversions++;
      
      // Method breakdown
      const method = conversion.attribution_method || 'unknown';
      analysis.methods_breakdown[method] = (analysis.methods_breakdown[method] || 0) + 1;
      
      // Confidence distribution
      const confidence = conversion.attribution_score || 0;
      if (confidence >= 280) analysis.confidence_distribution.high++;
      else if (confidence >= 200) analysis.confidence_distribution.medium++;
      else analysis.confidence_distribution.low++;
      
    } else {
      analysis.unattributed_conversions++;
    }
    
    // Dual IP analysis (from track.js data)
    if (conversion.dual_ip_scenario) {
      analysis.dual_ip_analysis.total++;
    }
    
    // Source performance
    const source = conversion.source || 'unknown';
    if (!analysis.source_performance[source]) {
      analysis.source_performance[source] = {
        conversions: 0,
        revenue: 0,
        attribution_rate: 0,
        attributed_count: 0
      };
    }
    
    analysis.source_performance[source].conversions++;
    analysis.source_performance[source].revenue += parseFloat(conversion.order_total) || 0;
    
    if (conversion.attribution_found) {
      analysis.source_performance[source].attributed_count++;
    }
    
    // IP extraction analysis (track.js provides full IP details)
    if (conversion.primary_ip) analysis.ip_extraction_analysis.primary_ip++;
    if (conversion.conversion_ip) analysis.ip_extraction_analysis.conversion_ip++;
    if (conversion.pageview_ip) analysis.ip_extraction_analysis.pageview_ip++;
  }
  
  // Calculate percentages
  analysis.dual_ip_analysis.percentage = analysis.total_conversions > 0 ? 
    ((analysis.dual_ip_analysis.total / analysis.total_conversions) * 100).toFixed(1) : '0.0';
  
  // Calculate attribution rates for sources
  Object.keys(analysis.source_performance).forEach(source => {
    const sourceData = analysis.source_performance[source];
    sourceData.attribution_rate = sourceData.conversions > 0 ? 
      ((sourceData.attributed_count / sourceData.conversions) * 100).toFixed(1) : '0.0';
  });
  
  // Sort source performance by revenue
  const sortedSources = Object.entries(analysis.source_performance)
    .sort(([,a], [,b]) => b.revenue - a.revenue)
    .slice(0, 10)
    .map(([source, data]) => ({ source, ...data }));
  
  analysis.source_performance = sortedSources;
  
  const processingTime = Date.now() - analysisStart;
  console.log(`✅ Real-time attribution analysis complete in ${processingTime}ms`);
  
  return {
    ...analysis,
    processing_time_ms: processingTime
  };
}

// Generate live analytics summary
function generateLiveAnalyticsSummary(conversions, attributionAnalysis) {
  console.log('📊 Generating live analytics summary...');
  
  // Calculate total revenue
  const totalRevenue = conversions.reduce((sum, conv) => 
    sum + (parseFloat(conv.order_total) || 0), 0
  );
  
  // Calculate attribution success rate
  const attributionSuccessRate = conversions.length > 0 ? 
    ((attributionAnalysis.attributed_conversions / conversions.length) * 100).toFixed(1) : '0.0';
  
  // Unique customers
  const uniqueCustomers = new Set(conversions.map(c => c.email).filter(Boolean)).size;
  
  return {
    total_revenue: totalRevenue,
    attribution_success_rate: attributionSuccessRate + '%',
    unique_customers: uniqueCustomers,
    keys_scanned: conversions._keysScanned || 0
  };
}

// Helper functions
function generateDatePatterns(startDate, endDate, includeToday) {
  const patterns = [];
  const start = new Date(startDate);
  const end = includeToday ? new Date() : new Date(endDate);
  
  // Generate patterns for each date
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const dateStr = d.toISOString().split('T')[0];
    patterns.push(`conversions:${dateStr}*`);
  }
  
  // Also include today's pattern with current timestamp format
  if (includeToday) {
    const today = new Date().toISOString().split('T')[0];
    patterns.push(`conversions:${today}*`);
    
    // Include timestamp-based pattern for today
    const now = new Date();
    const todayPrefix = now.getFullYear() + '-' + 
      String(now.getMonth() + 1).padStart(2, '0') + '-' + 
      String(now.getDate()).padStart(2, '0');
    patterns.push(`conversions:${todayPrefix}*`);
  }
  
  return [...new Set(patterns)]; // Remove duplicates
}

function isValidConversion(conversion) {
  // Validate essential fields (less strict than fast-analytics for real-time data)
  return conversion && 
         conversion.timestamp && 
         conversion.email && 
         conversion.order_id && 
         conversion.email !== 'unknown';
}

function extractConversionData(conversion, redisKey) {
  return {
    timestamp: conversion.timestamp,
    email: conversion.email,
    order_total: conversion.order_total || 0,
    order_id: conversion.order_id,
    event_type: conversion.event_type,
    
    // Attribution data from track.js
    attribution_found: conversion.attribution_found || false,
    attribution_method: conversion.attribution_method,
    attribution_score: conversion.attribution_score || 0,
    source: conversion.source,
    campaign: conversion.campaign,
    medium: conversion.medium,
    landing_page: conversion.landing_page,
    
    // Enhanced IP data from track.js
    primary_ip: conversion.primary_ip,
    conversion_ip: conversion.conversion_ip,
    pageview_ip: conversion.pageview_ip,
    ip_addresses_detected: conversion.ip_addresses_detected || 1,
    dual_ip_scenario: conversion.dual_ip_scenario || false,
    ip_version_mix: conversion.ip_version_mix,
    
    // Attribution tracking data
    session_id: conversion.session_id,
    device_signature: conversion.device_signature || conversion.dsig,
    screen_value: conversion.screen_value || conversion.SVV,
    gpu_signature: conversion.gpu_signature || conversion.gsig,
    
    // Metadata
    _redis_key: redisKey,
    _data_source: 'track_js_direct'
  };
}

function validateDate(dateStr) {
  if (!dateStr) return null;
  const regex = /^\d{4}-\d{2}-\d{2}$/;
  if (regex.test(dateStr)) {
    const date = new Date(dateStr);
    if (!isNaN(date.getTime())) {
      return dateStr;
    }
  }
  return null;
}

function validateLimit(limitStr) {
  if (!limitStr) return null;
  const limit = parseInt(limitStr);
  if (!isNaN(limit) && limit >= 1 && limit <= 10000) {
    return limit;
  }
  return null;
}

function getDateDaysAgo(daysAgo) {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return date.toISOString().split('T')[0];
}

function getDaysBetween(startDate, endDate) {
  const start = new Date(startDate);
  const end = new Date(endDate);
  return Math.round((end - start) / (1000 * 60 * 60 * 24)) + 1;
}

function isWithinDateRange(timestamp, startDate, endDate) {
  const date = new Date(timestamp).toISOString().split('T')[0];
  return date >= startDate && date <= endDate;
}

// Initialize Redis helper with aggressive timeouts for real-time performance
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 2000) => {
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
      if (error.name === 'AbortError') {
        throw new Error(`Redis timeout after ${timeoutMs}ms`);
      }
      throw error;
    }
  };
}
