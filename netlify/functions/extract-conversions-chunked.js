// extract-conversions-chunked.js - Build conversion indexes for fast-analytics
// Path: netlify/functions/extract-conversions-chunked.js

const headers = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Content-Type': 'application/json'
};

exports.handler = async (event, context) => {
  context.callbackWaitsForEmptyEventLoop = false;
  
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers };
  }

  try {
    console.log('💰 CONVERSION EXTRACTOR: Starting conversion index building...');
    const startTime = Date.now();
    const maxProcessingTime = 25000; // 25 seconds max
    
    const redis = initializeRedis();
    
    // Step 1: Find all conversion keys
    const conversionKeys = await findAllConversionKeys(redis);
    console.log(`📊 Found ${conversionKeys.length} conversion keys`);
    
    if (conversionKeys.length === 0) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: false,
          message: 'No conversion keys found'
        })
      };
    }
    
    // Step 2: Load and parse all conversions
    const allConversions = await loadAllConversions(redis, conversionKeys, maxProcessingTime - (Date.now() - startTime));
    console.log(`💰 Loaded ${allConversions.length} conversions`);
    
    // Step 3: Build date-based indexes for fast-analytics
    const indexResults = await buildConversionDateIndexes(redis, allConversions, maxProcessingTime - (Date.now() - startTime));
    
    const totalTime = Date.now() - startTime;
    console.log(`✅ Conversion indexing complete in ${totalTime}ms`);
    
    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({
        success: true,
        conversion_indexing_summary: {
          conversion_keys_found: conversionKeys.length,
          conversions_loaded: allConversions.length,
          date_indexes_created: indexResults.date_indexes_created,
          date_range_covered: indexResults.date_range,
          processing_time_ms: totalTime
        },
        indexing_performance: {
          conversions_per_second: Math.round(allConversions.length / (totalTime / 1000)),
          indexes_per_second: Math.round(indexResults.date_indexes_created / (totalTime / 1000))
        },
        next_steps: [
          'Test fast-analytics endpoint',
          'Verify conversion_index_date:* keys exist in Redis'
        ]
      })
    };
    
  } catch (error) {
    console.error('❌ Conversion extraction failed:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({ 
        error: 'Conversion extraction failed', 
        message: error.message 
      })
    };
  }
};

// Find all conversion keys in Redis
async function findAllConversionKeys(redis) {
  console.log('🔍 Scanning for conversion keys...');
  
  const conversionKeys = [];
  let cursor = '0';
  let iterations = 0;
  const maxIterations = 20;
  
  do {
    try {
      const scanResult = await redis(`scan/${cursor}/match/conversions:*/count/1000`);
      
      if (!scanResult?.result || !Array.isArray(scanResult.result) || scanResult.result.length < 2) {
        break;
      }
      
      cursor = scanResult.result[0];
      const keys = scanResult.result[1] || [];
      conversionKeys.push(...keys);
      iterations++;
      
      if (conversionKeys.length % 500 === 0) {
        console.log(`📊 Conversion scan progress: ${conversionKeys.length} keys found`);
      }
      
    } catch (scanError) {
      console.log(`⚠️ Conversion scan error: ${scanError.message}`);
      break;
    }
    
  } while (cursor !== '0' && iterations < maxIterations);
  
  console.log(`✅ Conversion scan complete: ${conversionKeys.length} keys found`);
  return conversionKeys;
}

// Load all conversions from Redis keys
async function loadAllConversions(redis, conversionKeys, maxTime) {
  const loadStartTime = Date.now();
  const conversions = [];
  
  console.log(`💰 Loading ${conversionKeys.length} conversions...`);
  
  const batchSize = 50;
  for (let i = 0; i < conversionKeys.length; i += batchSize) {
    if (Date.now() - loadStartTime > maxTime - 3000) {
      console.log(`⏰ Time limit reached while loading conversions`);
      break;
    }
    
    const batch = conversionKeys.slice(i, i + batchSize);
    
    try {
      const batchPromises = batch.map(async (key) => {
        try {
          const conversionData = await redis(`get/${key}`);
          if (conversionData?.result) {
            const conversion = JSON.parse(decodeURIComponent(conversionData.result));
            
            // Validate conversion has required fields
            if (conversion.timestamp && conversion.email) {
              return {
                timestamp: conversion.timestamp,
                email: conversion.email,
                order_total: conversion.order_total || 0,
                order_id: conversion.order_id,
                attribution_found: conversion.attribution_found || false,
                attribution_method: conversion.attribution_method,
                source: conversion.source,
                campaign: conversion.campaign,
                medium: conversion.medium,
                landing_page: conversion.landing_page,
                ip_address: conversion.ip_address,
                session_id: conversion.session_id,
                event_type: conversion.event_type,
                _redis_key: key
              };
            }
          }
        } catch (parseError) {
          console.warn(`⚠️ Failed to parse conversion ${key}`);
        }
        return null;
      });
      
      const batchResults = await Promise.all(batchPromises);
      const validResults = batchResults.filter(result => result !== null);
      conversions.push(...validResults);
      
      if ((i + batchSize) % 200 === 0) {
        console.log(`💰 Loading progress: ${conversions.length} conversions loaded from ${i + batchSize}/${conversionKeys.length} keys`);
      }
      
    } catch (batchError) {
      console.log(`⚠️ Error loading conversion batch ${Math.floor(i/batchSize) + 1}:`, batchError.message);
    }
  }
  
  // Sort by timestamp for consistent processing
  conversions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  console.log(`✅ Loaded ${conversions.length} valid conversions`);
  return conversions;
}

// Build date-based indexes that fast-analytics expects
async function buildConversionDateIndexes(redis, conversions, maxTime) {
  const indexStartTime = Date.now();
  console.log(`📅 Building date indexes for ${conversions.length} conversions...`);
  
  // Group conversions by date
  const dateGroups = {};
  let earliestDate = null;
  let latestDate = null;
  
  for (const conversion of conversions) {
    const date = new Date(conversion.timestamp);
    const dateKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
    
    if (!dateGroups[dateKey]) {
      dateGroups[dateKey] = [];
    }
    
    dateGroups[dateKey].push(conversion);
    
    // Track date range
    if (!earliestDate || date < earliestDate) earliestDate = date;
    if (!latestDate || date > latestDate) latestDate = date;
  }
  
  console.log(`📅 Grouped into ${Object.keys(dateGroups).length} date buckets`);
  
  // Create date indexes in Redis (format that fast-analytics expects)
  let dateIndexesCreated = 0;
  const dateEntries = Object.entries(dateGroups);
  
  for (const [dateKey, dateConversions] of dateEntries) {
    if (Date.now() - indexStartTime > maxTime - 1000) {
      console.log(`⏰ Time limit reached for date indexing`);
      break;
    }
    
    try {
      // This is the key format that fast-analytics.js expects!
      const indexKey = `conversion_index_date:${dateKey}`;
      
      const indexData = {
        date_key: dateKey,
        conversion_count: dateConversions.length,
        conversions: dateConversions, // Store all conversions for this date
        created_at: new Date().toISOString(),
        total_revenue: dateConversions.reduce((sum, conv) => sum + (parseFloat(conv.order_total) || 0), 0)
      };
      
      await redis(`setex/${indexKey}/7200/${encodeURIComponent(JSON.stringify(indexData))}`); // 2 hours TTL
      dateIndexesCreated++;
      
      if (dateIndexesCreated % 10 === 0) {
        console.log(`📅 Date indexing progress: ${dateIndexesCreated}/${dateEntries.length} indexes created`);
      }
      
    } catch (dateError) {
      console.log(`⚠️ Error creating date index for ${dateKey}:`, dateError.message);
    }
  }
  
  console.log(`✅ Date indexes: ${dateIndexesCreated} created`);
  
  return {
    date_indexes_created: dateIndexesCreated,
    date_range: {
      earliest: earliestDate?.toISOString(),
      latest: latestDate?.toISOString(),
      days_covered: Object.keys(dateGroups).length
    },
    processing_time_ms: Date.now() - indexStartTime
  };
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
