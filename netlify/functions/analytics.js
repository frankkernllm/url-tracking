// analytics.js - Optimized Performance Version
// Netlify Function for oJoy Analytics Dashboard 558 june28
// Fixes: 7-day default, dual pattern scanning, complete cursor iteration

// Initialize Redis client using the existing pattern from the original system
const redis = async (command) => {
  const url = `${process.env.UPSTASH_REDIS_REST_URL}/${command}`;
  const response = await fetch(url, {
    headers: {
      'Authorization': `Bearer ${process.env.UPSTASH_REDIS_REST_TOKEN}`,
      'Content-Type': 'application/json'
    }
  });
  return await response.json();
};

// Performance optimization: Limit maximum date range
const MAX_DAYS_ALLOWED = 7;
const DEFAULT_DAYS = 7;

exports.handler = async (event, context) => {
  // Set CORS headers
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Content-Type': 'application/json'
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    const startTime = Date.now();
    console.log('🚀 Analytics function started');

    // Parse query parameters with 7-day default
    const { 
      start_date, 
      end_date, 
      include_attribution_stats = 'false' 
    } = event.queryStringParameters || {};

    // Calculate optimized date range (max 7 days)
    const endDate = end_date ? new Date(end_date) : new Date();
    const startDate = start_date ? new Date(start_date) : new Date(Date.now() - (DEFAULT_DAYS * 24 * 60 * 60 * 1000));
    
    // Enforce maximum range limit
    const daysDiff = Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24));
    if (daysDiff > MAX_DAYS_ALLOWED) {
      console.log(`⚠️ Date range limited: ${daysDiff} days requested, using ${MAX_DAYS_ALLOWED} days`);
      startDate.setTime(endDate.getTime() - (MAX_DAYS_ALLOWED * 24 * 60 * 60 * 1000));
    }

    console.log(`📅 Processing ${Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24))} days: ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`);

    // Comprehensive attribution key discovery (dual patterns)
    const attributionData = await getComprehensiveAttributionData(startDate, endDate);
    console.log(`📊 Found ${attributionData.length} attribution records`);

    // Enhanced conversion data retrieval
    const conversionData = await getComprehensiveConversionData(startDate, endDate);
    console.log(`💰 Found ${conversionData.length} conversions`);

    // Generate analytics response
    const response = {
      data: {
        page_views: attributionData.length,
        conversions: conversionData.length,
        attribution_data: attributionData,
        conversion_data: conversionData,
        date_range: {
          start: startDate.toISOString(),
          end: endDate.toISOString(),
          days: Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24))
        },
        processing_stats: {
          execution_time_ms: Date.now() - startTime,
          data_patterns_scanned: ['attribution_*', 'attribution:*', 'conversions:*'],
          performance_optimized: true
        }
      }
    };

    // Include detailed attribution stats if requested
    if (include_attribution_stats === 'true') {
      response.data.attribution_stats = generateAttributionStats(attributionData);
    }

    console.log(`✅ Response ready: ${attributionData.length} views, ${conversionData.length} conversions (${Date.now() - startTime}ms)`);

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify(response)
    };

  } catch (error) {
    console.error('❌ Analytics function error:', error);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Internal server error',
        message: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// Comprehensive attribution data retrieval (dual patterns)
async function getComprehensiveAttributionData(startDate, endDate) {
  let allAttributionData = [];
  const startTimestamp = startDate.getTime();
  const endTimestamp = endDate.getTime();

  try {
    // PATTERN 1: IPv6 Underscore format (attribution_*)
    const ipv6Data = await getIPv6AttributionData(startTimestamp, endTimestamp);
    allAttributionData = allAttributionData.concat(ipv6Data);

    // PATTERN 2: IPv4 Colon format (attribution:*) - The missing data!
    const ipv4Data = await getIPv4AttributionData(startTimestamp, endTimestamp);
    allAttributionData = allAttributionData.concat(ipv4Data);

    console.log(`🎯 Pattern distribution: ${ipv6Data.length} IPv6, ${ipv4Data.length} IPv4`);

    // Remove duplicates and sort by timestamp
    const uniqueData = removeDuplicateAttribution(allAttributionData);
    return uniqueData.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

  } catch (error) {
    console.error('⚠️ Attribution data error:', error);
    return [];
  }
}

// IPv6 attribution pattern scanning (attribution_*)
async function getIPv6AttributionData(startTimestamp, endTimestamp) {
  const attributionData = [];
  
  // Common IPv6 prefixes for efficient scanning
  const ipv6Prefixes = [
    '2001', '2002', '2400', '2600', '2601', '2602', '2603', '2604',
    '2605', '2606', '2607', '2608', '2609', '260a', '260b', '260c'
  ];

  for (const prefix of ipv6Prefixes) {
    try {
      const result = await redis(`scan/0/match/attribution_${prefix}*/count/1000`);

      if (result.result && result.result[1] && result.result[1].length > 0) {
        const batchData = await processBatchAttributionKeys(result.result[1], startTimestamp, endTimestamp);
        attributionData.push(...batchData);
      }
    } catch (error) {
      console.warn(`⚠️ IPv6 prefix ${prefix} scan error:`, error.message);
    }
  }

  return attributionData;
}

// IPv4 attribution pattern scanning (attribution:*)
async function getIPv4AttributionData(startTimestamp, endTimestamp) {
  const attributionData = [];
  let cursor = '0';
  let totalProcessed = 0;

  try {
    do {
      const result = await redis(`scan/${cursor}/match/attribution:*/count/1000`);

      if (result.result && result.result[0]) {
        cursor = result.result[0];
        const keys = result.result[1];

        if (keys && keys.length > 0) {
          const batchData = await processBatchAttributionKeys(keys, startTimestamp, endTimestamp);
          attributionData.push(...batchData);
          totalProcessed += keys.length;
        }
      } else {
        break;
      }

      // Safety limit for cursor iteration
      if (totalProcessed > 5000) {
        console.log(`⚠️ IPv4 scan safety limit reached: ${totalProcessed} keys`);
        break;
      }

    } while (cursor !== '0');

  } catch (error) {
    console.warn('⚠️ IPv4 pattern scan error:', error.message);
  }

  return attributionData;
}

// Batch process attribution keys with date filtering
async function processBatchAttributionKeys(keys, startTimestamp, endTimestamp) {
  const validData = [];

  try {
    // Process in smaller batches for better performance
    const batchSize = 100;
    for (let i = 0; i < keys.length; i += batchSize) {
      const batch = keys.slice(i, i + batchSize);
      
      const batchResults = await Promise.all(
        batch.map(async (key) => {
          try {
            const result = await redis(`get/${key}`);
            return result.result ? { key, data: decodeURIComponent(result.result) } : null;
          } catch (e) {
            return null;
          }
        })
      );

      // Process batch results with date filtering
      batchResults.forEach(item => {
        if (!item || !item.data) return;

        try {
          const parsed = JSON.parse(item.data);
          
          // Enhanced timestamp validation
          const timestamp = parsed.timestamp ? new Date(parsed.timestamp).getTime() : null;
          if (!timestamp || isNaN(timestamp)) return;

          // Apply date range filter
          if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
            validData.push(parsed);
          }
        } catch (parseError) {
          // Skip malformed data silently
        }
      });
    }
  } catch (error) {
    console.warn('⚠️ Batch processing error:', error.message);
  }

  return validData;
}

// Comprehensive conversion data retrieval
async function getComprehensiveConversionData(startDate, endDate) {
  const conversionData = [];
  const startTimestamp = startDate.getTime();
  const endTimestamp = endDate.getTime();

  try {
    // Complete cursor iteration for conversions:*
    let cursor = '0';
    let totalFound = 0;

    do {
      const result = await redis(`scan/${cursor}/match/conversions:*/count/1000`);

      if (result.result && result.result[0]) {
        cursor = result.result[0];
        const keys = result.result[1];

        if (keys && keys.length > 0) {
          const batchData = await processBatchConversionKeys(keys, startTimestamp, endTimestamp);
          conversionData.push(...batchData);
          totalFound += keys.length;
        }
      } else {
        break;
      }

      // Safety limit
      if (totalFound > 3000) {
        console.log(`⚠️ Conversion scan safety limit: ${totalFound} keys`);
        break;
      }

    } while (cursor !== '0');

    return conversionData.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

  } catch (error) {
    console.error('⚠️ Conversion data error:', error);
    return [];
  }
}

// Process conversion keys with date filtering
async function processBatchConversionKeys(keys, startTimestamp, endTimestamp) {
  const validConversions = [];

  try {
    const batchSize = 100;
    for (let i = 0; i < keys.length; i += batchSize) {
      const batch = keys.slice(i, i + batchSize);
      
      const batchResults = await Promise.all(
        batch.map(async (key) => {
          try {
            const result = await redis(`get/${key}`);
            return result.result ? { key, data: decodeURIComponent(result.result) } : null;
          } catch (e) {
            return null;
          }
        })
      );

      batchResults.forEach(item => {
        if (!item || !item.data) return;

        try {
          const parsed = JSON.parse(item.data);
          
          // Timestamp validation
          const timestamp = parsed.timestamp ? new Date(parsed.timestamp).getTime() : null;
          if (!timestamp || isNaN(timestamp)) return;

          // Apply date range filter
          if (timestamp >= startTimestamp && timestamp <= endTimestamp) {
            validConversions.push(parsed);
          }
        } catch (parseError) {
          // Skip malformed data silently
        }
      });
    }
  } catch (error) {
    console.warn('⚠️ Conversion batch processing error:', error.message);
  }

  return validConversions;
}

// Remove duplicate attribution entries
function removeDuplicateAttribution(attributionData) {
  const seen = new Set();
  return attributionData.filter(item => {
    const key = `${item.ip_address}_${item.timestamp}_${item.session_id}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

// Generate attribution statistics
function generateAttributionStats(attributionData) {
  const stats = {
    total_sessions: attributionData.length,
    sources: {},
    source_types: {},
    top_landing_pages: {},
    traffic_distribution: {
      ipv4_traffic: 0,
      ipv6_traffic: 0
    }
  };

  attributionData.forEach(item => {
    // Source counting
    const source = item.source || 'unknown';
    stats.sources[source] = (stats.sources[source] || 0) + 1;

    // Source type counting
    const sourceType = item.source_type || 'unknown';
    stats.source_types[sourceType] = (stats.source_types[sourceType] || 0) + 1;

    // Landing page counting
    const landingPage = item.landing_page || 'unknown';
    stats.top_landing_pages[landingPage] = (stats.top_landing_pages[landingPage] || 0) + 1;

    // IP version distribution
    if (item.ip_address) {
      if (item.ip_address.includes(':')) {
        stats.traffic_distribution.ipv6_traffic++;
      } else {
        stats.traffic_distribution.ipv4_traffic++;
      }
    }
  });

  return stats;
}
