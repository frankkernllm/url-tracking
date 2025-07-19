// fast-analytics.js - ENHANCED VERSION with URL/Campaign Data & Email Filtering
// Path: netlify/functions/fast-analytics.js  
// Purpose: Ultra-fast analytics with URL tracking, campaign data, and clean email filtering

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('⚡ ENHANCED FAST-ANALYTICS: Starting with URL/campaign data & email filtering');
    const startTime = Date.now();
    const maxProcessingTime = 16000; // 16 seconds max (extra safety margin)
    
    // Parse query parameters
    const queryParams = event.queryStringParameters || {};
    const startDate = queryParams.start_date || getDateDaysAgo(7);
    const endDate = queryParams.end_date || getDateDaysAgo(0);
    const limit = parseInt(queryParams.limit || '2000'); // UPDATE 1: Increased from 500 to 2000
    
    console.log(`📅 ENHANCED: ${startDate} to ${endDate} (limit: ${limit})`);
    
    const redis = initializeRedis();
    
    // OPTIMIZATION 1: Try conversion indexes first (fastest path)
    console.log('🚀 FAST PATH: Trying conversion indexes with enhanced data...');
    const conversionData = await getConversionsFromIndexesFast(redis, startDate, endDate, Math.min(limit, 2000), 10000);
    
    if (conversionData.conversions.length > 0) {
      console.log(`✅ FAST PATH SUCCESS: ${conversionData.conversions.length} conversions with URL/campaign data`);
      
      const totalTime = Date.now() - startTime;
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          // Dashboard metrics
          total_revenue: conversionData.total_revenue.toFixed(2),
          total_conversions: conversionData.conversions.length,
          unique_visitors: conversionData.unique_customers,
          conversion_rate: conversionData.conversion_rate,
          
          // Data arrays
          page_views: [], // Empty for speed
          conversions: conversionData.conversions, // Return all found conversions
          
          // Analytics
          attribution_summary: conversionData.attribution_summary,
          
          // Meta
          date_range: { start: startDate, end: endDate },
          processing_stats: {
            execution_time_ms: totalTime,
            data_source: 'conversion_indexes_fast_enhanced',
            method: 'optimized_index_lookup_with_url_data',
            email_filtering_active: true,
            url_campaign_data_included: true
          }
        })
      };
    }
    
    // OPTIMIZATION 2: Fallback to limited journey scan (if indexes fail)
    console.log('⚠️ FALLBACK: Using limited journey scan with enhanced data...');
    const journeyData = await getJourneyDataLimited(redis, startDate, endDate, Math.min(limit, 500), maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    console.log(`⚡ ENHANCED analytics complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        // Dashboard metrics
        total_revenue: journeyData.total_revenue.toFixed(2),
        total_conversions: journeyData.total_conversions,
        unique_visitors: journeyData.unique_visitors,
        conversion_rate: journeyData.conversion_rate,
        
        // Data arrays
        page_views: [],
        conversions: journeyData.conversions,
        
        // Meta
        date_range: { start: startDate, end: endDate },
        processing_stats: {
          execution_time_ms: totalTime,
          data_source: 'limited_journey_scan_enhanced',
          method: 'timeout_optimized_with_url_data',
          email_filtering_active: true,
          url_campaign_data_included: true
        }
      })
    };
    
  } catch (error) {
    console.error('❌ Enhanced analytics error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Enhanced fast analytics failed', 
        message: error.message 
      })
    };
  }
};

// ENHANCED: Get conversions from date indexes with URL/campaign data and email filtering
async function getConversionsFromIndexesFast(redis, startDate, endDate, limit, maxTime) {
  const indexStartTime = Date.now();
  console.log(`🚀 ENHANCED INDEX LOOKUP: ${limit} conversions max, ${maxTime}ms timeout`);
  
  const conversions = [];
  let totalRevenue = 0;
  let attributedCount = 0;
  const uniqueCustomers = new Set();
  
  try {
    // Generate only the date keys we need
    const dateKeys = generateDateKeys(startDate, endDate);
    console.log(`📅 Checking ${dateKeys.length} date indexes with enhanced data extraction...`);
    
    // Process date indexes with strict timeout
    for (const dateKey of dateKeys) {
      if (Date.now() - indexStartTime > maxTime - 1000) {
        console.log('⏰ INDEX TIMEOUT: Stopping date index scan');
        break;
      }
      
      if (conversions.length >= limit) {
        console.log(`🔢 LIMIT REACHED: ${conversions.length} conversions`);
        break;
      }
      
      try {
        const indexKey = `conversion_index_date:${dateKey}`;
        const indexData = await redis(`get/${indexKey}`, 800); // Faster timeout for safety
        
        if (indexData?.result) {
          const dateIndex = JSON.parse(decodeURIComponent(indexData.result));
          
          if (dateIndex.conversions && Array.isArray(dateIndex.conversions)) {
            // UPDATE 2: Enhanced email filtering + take smaller batches for speed
            const dateConversions = dateIndex.conversions
              .filter(conv => conv.email && conv.order_id && conv.email !== 'unknown') // Enhanced email filter
              .slice(0, Math.min(200, limit - conversions.length)); // Reduced from 300 to 200
            
            for (const conversion of dateConversions) {
              conversions.push({
                // Original fields
                timestamp: conversion.timestamp,
                email: conversion.email,
                order_total: conversion.order_total || 0,
                order_id: conversion.order_id,
                source: conversion.source || 'direct',
                attribution_found: conversion.attribution_found || false,
                
                // UPDATE 1: URL and campaign data (zero performance impact)
                landing_page: conversion.landing_page || null,
                campaign: conversion.campaign || conversion.utm_campaign || null,
                medium: conversion.medium || conversion.utm_medium || null,
                utm_content: conversion.utm_content || null,
                utm_term: conversion.utm_term || null,
                referrer_url: conversion.referrer_url || null
              });
              
              totalRevenue += parseFloat(conversion.order_total || 0);
              uniqueCustomers.add(conversion.email);
              
              if (conversion.attribution_found) {
                attributedCount++;
              }
            }
          }
        }
        
      } catch (indexError) {
        console.warn(`⚠️ Error loading enhanced index ${dateKey}:`, indexError.message);
      }
    }
    
    const conversionRate = uniqueCustomers.size > 0 ? 
      ((conversions.length / uniqueCustomers.size) * 100).toFixed(2) : '0.00';
    
    const attributionRate = conversions.length > 0 ? 
      ((attributedCount / conversions.length) * 100).toFixed(1) : '0.0';
    
    console.log(`✅ ENHANCED INDEX SCAN: ${conversions.length} conversions with URL data in ${Date.now() - indexStartTime}ms`);
    
    return {
      conversions: conversions,
      total_revenue: totalRevenue,
      unique_customers: uniqueCustomers.size,
      conversion_rate: conversionRate,
      attribution_summary: {
        total_conversions: conversions.length,
        attributed_conversions: attributedCount,
        attribution_rate: attributionRate
      }
    };
    
  } catch (error) {
    console.error('❌ Enhanced index scan error:', error);
    return {
      conversions: [],
      total_revenue: 0,
      unique_customers: 0,
      conversion_rate: '0.00',
      attribution_summary: {
        total_conversions: 0,
        attributed_conversions: 0,
        attribution_rate: '0.0'
      }
    };
  }
}

// ENHANCED: Limited journey scan with URL data and email filtering (fallback)
async function getJourneyDataLimited(redis, startDate, endDate, limit, maxTime) {
  const scanStartTime = Date.now();
  console.log(`🔄 ENHANCED LIMITED JOURNEY SCAN: ${limit} journeys max, ${maxTime}ms timeout`);
  
  const conversions = [];
  let totalRevenue = 0;
  const uniqueCustomers = new Set();
  
  const startTimestamp = new Date(startDate).getTime();
  const endTimestamp = new Date(endDate + 'T23:59:59').getTime();
  
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 12; // Reduced from 15 for extra safety
  
  try {
    do {
      if (Date.now() - scanStartTime > maxTime - 2000) {
        console.log('⏰ JOURNEY TIMEOUT: Stopping scan');
        break;
      }
      
      const scanResult = await redis(`scan/${cursor}/match/customer_journey:*/count/50`, 1200); // Slightly faster timeout
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      iterations++;
      
      // Process smaller batches per iteration for faster processing
      const limitedKeys = keys.slice(0, Math.min(30, limit - conversions.length)); // Reduced from 50 to 30
      
      const batchPromises = limitedKeys.map(async (key) => {
        try {
          const journeyData = await redis(`get/${key}`, 800);
          if (journeyData?.result) {
            const journey = JSON.parse(decodeURIComponent(journeyData.result));
            
            // Quick date filter
            const conversionTime = new Date(journey.conversion_timestamp).getTime();
            if (conversionTime >= startTimestamp && conversionTime <= endTimestamp) {
              return {
                // Original fields
                timestamp: journey.conversion_timestamp,
                email: journey.customer_email,
                order_total: journey.conversion_value || 0,
                order_id: journey.conversion_order_id,
                source: journey.first_click_source || 'direct',
                attribution_found: journey.total_touchpoints > 1,
                
                // Enhanced fields from journey touchpoints (if available)
                landing_page: journey.touchpoints?.[0]?.landing_page || null,
                campaign: journey.touchpoints?.[0]?.campaign || null,
                medium: journey.touchpoints?.[0]?.medium || null,
                utm_content: journey.touchpoints?.[0]?.utm_content || null,
                utm_term: journey.touchpoints?.[0]?.utm_term || null,
                referrer_url: journey.touchpoints?.[0]?.referrer_url || null
              };
            }
          }
        } catch (parseError) {
          // Skip invalid data
        }
        return null;
      });
      
      const batchResults = await Promise.all(batchPromises);
      const validJourneys = batchResults
        .filter(j => j !== null)
        .filter(journey => journey.email && journey.email !== 'unknown'); // UPDATE 2: Enhanced email filtering
      
      for (const journey of validJourneys) {
        conversions.push(journey);
        totalRevenue += parseFloat(journey.order_total);
        uniqueCustomers.add(journey.email);
        
        if (conversions.length >= limit) break;
      }
      
      if (conversions.length >= limit) break;
      
    } while (cursor !== '0' && iterations < maxIterations);
    
    const conversionRate = uniqueCustomers.size > 0 ? 
      ((conversions.length / uniqueCustomers.size) * 100).toFixed(2) : '0.00';
    
    console.log(`✅ ENHANCED LIMITED SCAN: ${conversions.length} conversions with URL data in ${Date.now() - scanStartTime}ms`);
    
    return {
      conversions: conversions,
      total_conversions: conversions.length,
      total_revenue: totalRevenue,
      unique_visitors: uniqueCustomers.size,
      conversion_rate: conversionRate
    };
    
  } catch (error) {
    console.error('❌ Enhanced limited journey scan error:', error);
    return {
      conversions: [],
      total_conversions: 0,
      total_revenue: 0,
      unique_visitors: 0,
      conversion_rate: '0.00'
    };
  }
}

// Helper functions
function getDateDaysAgo(daysAgo) {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  return date.toISOString().split('T')[0];
}

function generateDateKeys(startDate, endDate) {
  const keys = [];
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    keys.push(key);
  }
  
  return keys;
}

// Initialize Redis helper with faster timeouts
function initializeRedis() {
  const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
  const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
  
  return async (command, timeoutMs = 1200) => { // Reduced from 1500ms for faster fail
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
